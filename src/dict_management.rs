use anyhow::Context as AContext;
use rusqlite::{functions::Context, params};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use zstd::dict::{DecoderDictionary, EncoderDictionary};

// TODO: the rust interface currently requires a level when preparing a dictionary, but the zstd interface (ZSTD_CCtx_loadDictionary) does not.
// TODO: Using LruCache here isn't very smart
pub fn encoder_dict_from_ctx<'a>(
    ctx: &'a Context,
    arg_index: usize,
    level: i32,
) -> anyhow::Result<Arc<EncoderDictionary<'static>>> {
    use lru_time_cache::LruCache;
    // we cache the instantiated encoder dictionaries keyed by (DbConnection, dict_id, compression_level)
    // DbConnection would ideally be db.path() because it's the same for multiple connections to the same db, but that would be less robust (e.g. in-memory databases)
    lazy_static::lazy_static! {
        static ref DICTS: RwLock<LruCache<(usize, i32, i32), Arc<EncoderDictionary<'static>>>> = RwLock::new(LruCache::with_expiry_duration(Duration::from_secs(10)));
    }
    let id: i32 = ctx.get(arg_index)?;
    let db = unsafe { ctx.get_connection()? }; // SAFETY: This might be unsafe depending on how the connection is used. See https://github.com/rusqlite/rusqlite/issues/643#issuecomment-640181213
    let db_handle_pointer = unsafe { db.handle() } as usize; // SAFETY: We're only getting the pointer as an int, not using the raw connection

    let mut dicts_write = DICTS.write().unwrap();
    let entry = dicts_write.entry((db_handle_pointer, id, level));
    let res = match entry {
        lru_time_cache::Entry::Vacant(e) => e.insert({
            log::debug!(
                "loading encoder dictionary {} level {} (should only happen once per 10s)",
                id,
                level
            );

            let dict_raw: Vec<u8> = db
                .query_row(
                    "select dict from _zstd_dicts where id = ?",
                    params![id],
                    |r| r.get(0),
                )
                .with_context(|| format!("getting dict with id={} from _zstd_dicts", id))?;
            let dict = EncoderDictionary::copy(&dict_raw, level);
            Arc::new(dict)
        }),
        lru_time_cache::Entry::Occupied(o) => o.into_mut(),
    }
    .clone();
    Ok(res)
}

pub fn decoder_dict_from_ctx<'a>(
    ctx: &'a Context,
    arg_index: usize,
) -> anyhow::Result<Arc<DecoderDictionary<'static>>> {
    use lru_time_cache::LruCache;
    // we cache the instantiated decoder dictionaries keyed by (DbConnection, dict_id)
    // DbConnection would ideally be db.path() because it's the same for multiple connections to the same db, but that would be less robust (e.g. in-memory databases)
    lazy_static::lazy_static! {
        static ref DICTS: RwLock<LruCache<(usize, i32), Arc<DecoderDictionary<'static>>>> = RwLock::new(LruCache::with_expiry_duration(Duration::from_secs(10)));
    }
    let id: i32 = ctx.get(arg_index)?;
    let db = unsafe { ctx.get_connection()? }; // SAFETY: This might be unsafe depending on how the connection is used. See https://github.com/rusqlite/rusqlite/issues/643#issuecomment-640181213
    let db_handle_pointer = unsafe { db.handle() } as usize; // SAFETY: We're only getting the pointer as an int, not using the raw connection
    let mut dicts_write = DICTS.write().unwrap();
    let entry = dicts_write.entry((db_handle_pointer, id));
    let res = match entry {
        lru_time_cache::Entry::Vacant(e) => e.insert({
            log::debug!(
                "loading decoder dictionary {} (should only happen once per 10s)",
                id
            );
            let db = unsafe { ctx.get_connection()? };
            let dict_raw: Vec<u8> = db
                .query_row(
                    "select dict from _zstd_dicts where id = ?",
                    params![id],
                    |r| r.get(0),
                )
                .with_context(|| format!("getting dict with id={} from _zstd_dicts", id))?;
            let dict = DecoderDictionary::copy(&dict_raw);
            Arc::new(dict)
        }),
        lru_time_cache::Entry::Occupied(o) => o.into_mut(),
    }
    .clone();
    Ok(res)
}

/*


use rusqlite::{functions::Context, params, types::ValueRef};

/// load a dict from sqlite function parameters
///
/// sqlite sadly does not do auxdata caching for subqueries like `zstd_compress(data, 3, (select dict from _zstd_dicts where id = 4))`
/// so instead we support the syntax `zstd_compress(data, 3, 4)` as an alias to the above
/// if the dict parameter is a number, the dict will be queried from the _zstd_dicts table and cached in sqlite auxdata
/// so it is only constructed once per query
///
/// this function is not 100% correct because the level is passed separately from the dictionary but the dictionary is cached in the aux data of the dictionary parameter
/// e.g. `select zstd_compress(tbl.data, tbl.row_compression_level, 123) from tbl` will probably compress all the data with the same compression ratio instead of a random one
/// as a workaround `select zstd_compress(tbl.data, tbl.row_compression_level, (select 123)) from tbl` probably works
/// to fix this the level parameter would need to be checked against the constructed dictionary and the dict discarded on mismatch
pub fn encoder_dict_from_ctx<'a>(
    ctx: &'a Context,
    arg_index: usize,
    level: i32,
) -> rusqlite::Result<Arc<OwnedEncoderDict<'a>>> {
    Ok(match ctx.get_aux::<OwnedEncoderDict>(arg_index as i32)? {
        Some(d) => d,
        None => {
            log::debug!("loading dictionary (should only happen once per query)");
            let dict_raw = match ctx.get_raw(arg_index) {
                ValueRef::Blob(b) => b.to_vec(),
                ValueRef::Integer(i) => {
                    let db = unsafe { ctx.get_connection()? };
                    let res: Vec<u8> = db.query_row(
                        "select dict from _zstd_dicts where id = ?",
                        params![i],
                        |r| r.get(0),
                    )?;
                    res
                }
                e => {
                    return Err(rusqlite::Error::InvalidFunctionParameterType(
                        arg_index,
                        e.data_type(),
                    ))
                }
            };
            let dict = wrap_encoder_dict(dict_raw, level);
            ctx.set_aux(arg_index as i32, dict)?;
            ctx.get_aux::<OwnedEncoderDict>(arg_index as i32)?.unwrap()
        }
    })
}


/// same as above
pub fn decoder_dict_from_ctx<'a>(
    ctx: &'a Context,
    arg_index: usize,
) -> rusqlite::Result<Arc<OwnedDecoderDict<'a>>> {
    Ok(match ctx.get_aux::<OwnedDecoderDict>(arg_index as i32)? {
        Some(d) => d,
        None => {
            log::debug!("loading dictionary (should only happen once per query)");
            let dict_raw = /*ctx.get::<Vec<u8>>(arg_index)?;*/
            match ctx.get_raw(arg_index) {
                ValueRef::Blob(b) => b.to_vec(),
                ValueRef::Integer(i) => {
                    let db = unsafe { ctx.get_connection()? };
                    let res: Vec<u8> = db.query_row(
                        "select dict from _zstd_dicts where id = ?",
                        params![i],
                        |r| r.get(0),
                    )?;
                    res
                }
                e => return Err(rusqlite::Error::InvalidFunctionParameterType(
                    arg_index,
                    e.data_type(),
                )),
            };
            let dict = wrap_decoder_dict(dict_raw);
            ctx.set_aux(arg_index as i32, dict)?;
            ctx.get_aux::<OwnedDecoderDict>(arg_index as i32)?.unwrap()
        }
    })
}
*/
