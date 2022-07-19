use crate::dict_management::*;
use anyhow::Context as AContext;

use rusqlite::functions::Context;

use rusqlite::types::ToSqlOutput;
use rusqlite::types::{Value, ValueRef};
use std::{io::Write, sync::Arc};
use zstd::bulk::Compressor;
use zstd::dict::DecoderDictionary;

/// null_dict_is_passthrough is only true when called through the `zstd_compress_col` function (for transparent compression)
/// with null_dict_is_passthrough, the behaviour is slightly changed: When dict is null, the data is passed through without compression.
pub(crate) fn zstd_compress_fn<'a>(
    ctx: &Context,
    null_dict_is_passthrough: bool,
) -> anyhow::Result<ToSqlOutput<'a>> {
    let arg_data = 0;
    let arg_level = 1;
    let arg_dict = 2;
    let arg_is_compact = 3;

    let input_value = match ctx.get_raw(arg_data) {
        ValueRef::Blob(b) => b,
        ValueRef::Text(b) => b,
        ValueRef::Null => return Ok(ToSqlOutput::Owned(Value::Null)), // pass through null
        e => {
            anyhow::bail!(
                "zstd_compress expects blob or text as input, got {}",
                e.data_type()
            )
        }
    };

    if null_dict_is_passthrough && ctx.len() >= arg_dict {
        // if the dict id is null, pass through data
        if let ValueRef::Null = ctx.get_raw(arg_dict) {
            // TODO: figure out if sqlite3_result_blob can be passed a pointer into sqlite3_context to avoid copying??
            // return Ok(ToSqlOutput::Borrowed(ctx.get_raw(arg_data)));
            return Ok(ToSqlOutput::Owned(Value::Blob(input_value.to_vec())));
        }
    }

    let level: i32 = if ctx.len() <= arg_level {
        // no level given, use default (currently 3)
        0
    } else {
        ctx.get(arg_level).context("level argument")?
    };
    let compact: bool = if ctx.len() <= arg_is_compact {
        false
    } else {
        ctx.get(arg_is_compact).context("is_compact argument")?
    };

    if ctx.len() <= arg_dict {
        zstd_compress_fn_tail(compact, input_value, Compressor::new(level))
    } else {
        match ctx.get_raw(arg_dict) {
            ValueRef::Integer(-1) | ValueRef::Null => {
                zstd_compress_fn_tail(compact, input_value, Compressor::new(level))
            }
            ValueRef::Blob(d) => {
                zstd_compress_fn_tail(compact, input_value, Compressor::with_dictionary(level, d))
            }
            //Some(Arc::new(wrap_encoder_dict(d.to_vec(), level))),
            ValueRef::Integer(_) => {
                let dick = encoder_dict_from_ctx(ctx, arg_dict, level)
                    .context("loading dictionary from int")?;

                let enc = Compressor::with_prepared_dictionary(&dick);
                zstd_compress_fn_tail(compact, input_value, enc)
            }
            other => anyhow::bail!(
                "dict argument must be int or blob, got {}",
                other.data_type()
            ),
        }
    }
}

// separate fn purely for borrowship simplicity
fn zstd_compress_fn_tail<'a>(
    compact: bool,
    input_value: &[u8],
    encoder: Result<Compressor, std::io::Error>,
) -> anyhow::Result<ToSqlOutput<'a>> {
    let mut encoder = encoder.context("creating zstd encoder")?;
    {
        // pledge source size (benchmarking shows this doesn't help any tho)
        let cctx = encoder.context_mut();
        cctx.set_pledged_src_size(input_value.len() as u64)
            .map_err(|c| anyhow::anyhow!("setting pledged source size (code {c})"))?;
        // cctx.set_parameter(zstd::zstd_safe::CParameter::BlockDelimiters(false))
        //    .map_err(|_| anyhow::anyhow!("no"))?;
    }
    if compact {
        encoder
            .include_checksum(false)
            .context("disable checksums")?;
        encoder.include_contentsize(false).context("cs")?;
        encoder.include_dictid(false).context("did")?;
        encoder.include_magicbytes(false).context("did")?;
    }
    let res = encoder
        .compress(input_value)
        .context("writing data to zstd encoder")?;

    Ok(ToSqlOutput::Owned(Value::Blob(res)))
}

pub(crate) fn zstd_decompress_fn<'a>(
    ctx: &Context,
    null_dict_is_passthrough: bool,
) -> anyhow::Result<ToSqlOutput<'a>> {
    let arg_data = 0;
    let arg_output_text = 1;
    let arg_dict = 2;
    let arg_is_compact = 3;

    if null_dict_is_passthrough && ctx.len() >= arg_dict {
        // if the dict id is null, pass through data

        if let ValueRef::Null = ctx.get_raw(arg_dict) {
            // TODO: figure out if sqlite3_result_blob can be passed a pointer into sqlite3_context to avoid copying??
            // return Ok(ToSqlOutput::Borrowed(ctx.get_raw(arg_data)));
            return Ok(ToSqlOutput::Owned(ctx.get_raw(arg_data).into()));
        }
    }

    let output_text: bool = ctx.get(arg_output_text).context("output_text arg")?;

    let input_value = match ctx.get_raw(arg_data) {
        ValueRef::Blob(b) => b,
        ValueRef::Null => return Ok(ToSqlOutput::Owned(Value::Null)), // pass through null
        e => {
            anyhow::bail!(
                "zstd_decompress expects blob as input, got {}",
                e.data_type()
            )
        }
    };

    let dict = if ctx.len() <= arg_dict {
        None
    } else {
        match ctx.get_raw(arg_dict) {
            ValueRef::Integer(-1) | ValueRef::Null => None,
            ValueRef::Blob(d) => Some(Arc::new(DecoderDictionary::copy(d))),
            ValueRef::Integer(_) => {
                Some(decoder_dict_from_ctx(ctx, arg_dict).context("load dict")?)
            }
            other => anyhow::bail!(
                "dict argument must be int or blob, got {}",
                other.data_type()
            ),
        }
    };

    let compact = if ctx.len() <= arg_is_compact {
        false
    } else {
        ctx.get(arg_is_compact).context("argument 'compact'")?
    };
    let dict_ref = dict.as_ref().map(|e| -> &DecoderDictionary { e });

    zstd_decompress_inner(input_value, dict_ref, output_text, compact)
}

fn zstd_decompress_inner<'a>(
    input_value: &[u8],
    dict: Option<&DecoderDictionary>,
    output_text: bool,
    compact: bool,
) -> anyhow::Result<ToSqlOutput<'a>> {
    let vec = {
        // todo: use zstd::bulk api maybe (but we don't know the output size)
        let out = Vec::new();
        let mut decoder = match &dict {
            Some(dict) => zstd::stream::write::Decoder::with_prepared_dictionary(out, dict),
            None => zstd::stream::write::Decoder::new(out),
        }
        .context("dict load doesn't work")?;
        if compact {
            decoder.include_magicbytes(false)?;
        }
        decoder.write_all(input_value).context("decoding")?;
        decoder.flush().context("decoder flushing")?;
        decoder.into_inner()
    };

    // dict; // to make sure the dict is still in scope because of https://github.com/gyscos/zstd-rs/issues/55
    if output_text {
        Ok(ToSqlOutput::Owned(Value::Text(
            // converted right back to &u8 in https://docs.rs/rusqlite/0.21.0/src/rusqlite/types/value_ref.rs.html#107
            // so we don't want the overhead of checking utf8. also db encoding might not be utf8 so ??
            unsafe { String::from_utf8_unchecked(vec) },
        )))
    } else {
        Ok(ToSqlOutput::Owned(Value::Blob(vec)))
    }
}
