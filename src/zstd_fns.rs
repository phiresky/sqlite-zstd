use crate::dict_management::*;
use crate::transparent::*;
use crate::util::*;
use anyhow::Context as AContext;
use rand::Rng;
use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::ToSql;
use rusqlite::types::ToSqlOutput;
use rusqlite::types::{Value, ValueRef};
use rusqlite::{params, Connection};
use std::{io::Write, sync::Mutex};

pub fn ensure_dicts_table_exists(db: &Connection) -> rusqlite::Result<()> {
    db.execute(
        "
        create table if not exists _zstd_dicts (
            id integer primary key autoincrement,
            chooser_key text unique not null,
            dict blob not null
        );",
        params![],
    )?;
    Ok(())
}

struct ZstdTrainDictAggregate {
    // if None, return trained dict, otherwise insert into _zstd_dicts table with chooser_key given as fourth arg and return id
    // if false expects 3 args, if true expects 4 args
    return_save_id: bool,
}
struct ZstdTrainDictState {
    // this shouldn't be stored here at all, but rusqlite currently has no context access in finish function. the mutex is really just a hack to allow carrying across unwind boundary
    db: Mutex<Connection>,
    reservoir: Vec<Vec<u8>>,
    wanted_item_count: usize,
    total_count: usize,
    wanted_dict_size: usize,
    chooser_key: Option<String>,
}

impl rusqlite::functions::Aggregate<Option<ZstdTrainDictState>, Value> for ZstdTrainDictAggregate {
    fn init(&self) -> Option<ZstdTrainDictState> {
        // TODO: PR to rusqlite library that passes context to init fn
        None
    }
    fn step(
        &self,
        ctx: &mut Context,
        state: &mut Option<ZstdTrainDictState>,
    ) -> rusqlite::Result<()> {
        let arg_sample = 0;
        let arg_dict_size_bytes = 1;
        let arg_sample_count = 2;
        let arg_chooser_key = 3;
        if state.is_none() {
            state.replace(ZstdTrainDictState {
                reservoir: vec![],
                wanted_item_count: ctx.get::<f64>(arg_sample_count)? as usize,
                wanted_dict_size: ctx.get::<i64>(arg_dict_size_bytes)? as usize,
                total_count: 0,
                db: Mutex::new(unsafe { ctx.get_connection()? }),
                chooser_key: if self.return_save_id {
                    Some(ctx.get(arg_chooser_key)?)
                } else {
                    None
                },
            });
            log::debug!(
                "sampling {} values",
                state.as_ref().map(|e| e.wanted_item_count).unwrap_or(0)
            );
        }
        let mut state = state.as_mut().unwrap();
        let cur = match ctx.get_raw(arg_sample) {
            ValueRef::Blob(b) => b,
            ValueRef::Text(b) => b,
            ValueRef::Real(_f) => return Ok(()),
            ValueRef::Integer(_i) => return Ok(()),
            ValueRef::Null => return Ok(()),
        };
        let i = state.total_count;
        let k = state.wanted_item_count;
        // https://en.wikipedia.org/wiki/Reservoir_sampling#Simple_algorithm

        if i < k {
            state.reservoir.push(Vec::from(cur));
            state.total_count += 1;
            return Ok(());
        }
        state.total_count += 1;
        let j = rand::thread_rng().gen_range(0, i);
        if j < k {
            state.reservoir[j] = Vec::from(cur);
        }
        Ok(())
    }

    fn finalize(&self, state: Option<Option<ZstdTrainDictState>>) -> rusqlite::Result<Value> {
        let state = state
            .flatten()
            .ok_or(ah(anyhow::anyhow!("tried to train zstd dict on zero rows")))?;
        log::debug!(
            "training dict of size {}kB with {} samples (of {} seen)",
            state.wanted_dict_size / 1000,
            state.reservoir.len(),
            state.total_count
        );
        let dict = zstd::dict::from_samples(&state.reservoir, state.wanted_dict_size)
            .context("Training dictionary failed")
            .map_err(ah)?;
        if let Some(key) = state.chooser_key {
            let db = &state.db.lock().unwrap();
            ensure_dicts_table_exists(db)?;
            db.execute(
                "insert into _zstd_dicts (chooser_key,dict) values (?, ?);",
                params![key, dict],
            )?;
            let id = db.last_insert_rowid();
            log::debug!("inserted dict into _zstd_dicts with key {}, id {}", key, id);
            Ok(Value::Integer(id))
        } else {
            Ok(Value::Blob(dict))
        }
    }
}

fn zstd_compress(ctx: &Context) -> Result<Box<dyn ToSql>, rusqlite::Error> {
    let arg_data = 0;
    let arg_level = 1;
    let arg_dict = 2;
    let (is_blob, input_value) = match ctx.get_raw(arg_data) {
        ValueRef::Blob(b) => (true, b),
        ValueRef::Text(b) => (false, b),
        // pass through data that is not compressible anyways
        // this is useful because sqlite does not enforce any types so a column of type text or blob can still contain integers etc
        ValueRef::Real(f) => return Ok(Box::new(f)),
        ValueRef::Integer(i) => return Ok(Box::new(i)),
        ValueRef::Null => return Ok(Box::new(Option::<i32>::None)),
    };
    let level = if ctx.len() < 2 {
        // no level given, use default (currently 3)
        0
    } else {
        ctx.get::<i32>(arg_level)?
    };
    let dict = if ctx.len() < 3 {
        // no third argument -> no dict
        None
    } else {
        Some(encoder_dict_from_ctx(&ctx, arg_dict, level)?)
    };

    let is_blob: &[u8] = if is_blob { b"b" } else { b"s" };
    let res = {
        let out = Vec::new();
        let mut encoder = match dict {
            Some(dict) => zstd::stream::write::Encoder::with_prepared_dictionary(out, &dict),
            None => zstd::stream::write::Encoder::new(out, level),
        }
        .context("creating zstd encoder")
        .map_err(ah)?;
        encoder
            .write_all(input_value)
            .context("writing data to zstd encoder")
            .map_err(ah)?;
        encoder.write_all(is_blob).context("blob").map_err(ah)?;
        encoder
            .finish()
            .context("finishing zstd stream")
            .map_err(ah)?
    };
    Ok(Box::new(res))
}

fn zstd_decompress_col<'a>(ctx: &Context) -> Result<ToSqlOutput<'a>, rusqlite::Error> {
    let arg_data = 0;
    let arg_dict = 1;
    // if the dict id is null, pass through data
    if let ValueRef::Null = ctx.get_raw(arg_dict) {
        // TODO: figure out if sqlite3_result_blob can be passed a pointer into sqlite3_context??
        // return Ok(ToSqlOutput::Borrowed(ctx.get_raw(arg_data)));
        return Ok(ToSqlOutput::Owned(ctx.get_raw(arg_data).into()));
    }
    let input_value = match ctx.get_raw(arg_data) {
        ValueRef::Blob(b) => b,
        ValueRef::Text(_b) => {
            return Err(ah(anyhow::anyhow!(
                "got string, but zstd compressed data is always blob"
            )))
        }
        ValueRef::Real(f) => return Ok(ToSqlOutput::Owned(Value::Real(f))),
        ValueRef::Integer(i) => return Ok(ToSqlOutput::Owned(Value::Integer(i))),
        ValueRef::Null => return Ok(ToSqlOutput::Owned(Value::Null)),
    };

    let dict_raw = Some(decoder_dict_from_ctx(&ctx, arg_dict)?);

    let mut vec = {
        let out = Vec::new();
        let mut decoder = match dict_raw {
            Some(dict) => zstd::stream::write::Decoder::with_prepared_dictionary(out, &dict),
            None => zstd::stream::write::Decoder::new(out),
        }
        .context("dict load doesn't work")
        .map_err(ah)?;
        decoder
            .write_all(input_value)
            .context("decoding")
            .map_err(ah)?;
        decoder.flush().context("decoder flushing").map_err(ah)?;
        decoder.into_inner()
    };

    let is_blob = vec.pop().unwrap();
    if is_blob == b'b' {
        Ok(ToSqlOutput::Owned(Value::Blob(vec)))
    } else {
        Ok(ToSqlOutput::Owned(Value::Text(
            // converted right back to &u8 in https://docs.rs/rusqlite/0.21.0/src/rusqlite/types/value_ref.rs.html#107
            // so we don't want the overhead of checking utf8
            unsafe { String::from_utf8_unchecked(vec) },
        )))
    }
    // let dictionary
}

pub fn add_functions(db: &rusqlite::Connection) -> anyhow::Result<()> {
    let nondeterministic = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY;
    let deterministic = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    //
    db.create_scalar_function("zstd_compress", 1, deterministic, zstd_compress)?;
    db.create_scalar_function("zstd_compress", 2, deterministic, zstd_compress)?;
    db.create_scalar_function("zstd_compress", 3, deterministic, zstd_compress)?;
    //db.create_scalar_function("zstd_decompress", 1, deterministic, zstd_decompress)?;
    //db.create_scalar_function("zstd_decompress", 2, deterministic, zstd_decompress)?;
    db.create_scalar_function("zstd_decompress_col", 2, deterministic, zstd_decompress_col)?;
    db.create_aggregate_function(
        "zstd_train_dict",
        3,
        nondeterministic,
        ZstdTrainDictAggregate {
            return_save_id: false,
        },
    )?;
    db.create_aggregate_function(
        "zstd_train_dict_and_save",
        4,
        nondeterministic,
        ZstdTrainDictAggregate {
            return_save_id: true,
        },
    )?;
    db.create_scalar_function("zstd_enable_transparent", 1, nondeterministic, |ctx| {
        zstd_enable_transparent(ctx).map_err(ah)
    })?;

    db.create_scalar_function("zstd_transparent_maintenance", 1, nondeterministic, |ctx| {
        zstd_transparent_maintenance(ctx).map_err(ah)
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    // the point of this is that it's something you might store in a DB that has lots of redundant data
    #[derive(Serialize, Deserialize, Debug)]
    #[serde(tag = "type")]
    enum EventData {
        OpenApplication {
            id: i32,
            app_name: String,
            app_type: String,
            properties: BTreeMap<String, String>,
        },
        CloseApplication {
            id: i32,
        },
        Shutdown,
    }

    fn create_example_db() -> anyhow::Result<Connection> {
        let mut db = Connection::open_in_memory()?;
        add_functions(&db)?;
        db.execute_batch(
            "
            create table events (
                id integer primary key not null,
                timestamp text not null,
                data json not null
            );
        ",
        )?;

        // people use maybe 100 different apps
        let app_names: Vec<String> = names::Generator::with_naming(names::Name::Plain)
            .take(100)
            .collect();
        // of maybe 10 different categories
        let app_types: Vec<String> = names::Generator::with_naming(names::Name::Plain)
            .take(10)
            .collect();
        use rand::distributions::WeightedIndex;
        use rand::prelude::*;

        let window_properties = &[
            (30, "_GTK_APPLICATION_ID"),
            (30, "_GTK_APPLICATION_OBJECT_PATH"),
            (30, "_GTK_UNIQUE_BUS_NAME"),
            (30, "_GTK_WINDOW_OBJECT_PATH"),
            (40, "_NET_WM_USER_TIME_WINDOW"),
            (41, "WM_CLIENT_LEADER"),
            (50, "_NET_WM_BYPASS_COMPOSITOR"),
            (60, "WM_WINDOW_ROLE"),
            (61, "_MOTIF_WM_HINTS"),
            (90, "_GTK_THEME_VARIANT"),
            (91, "_NET_WM_SYNC_REQUEST_COUNTER"),
            (91, "_NET_WM_USER_TIME"),
            (139, "_NET_STARTUP_ID"),
            (170, "_NET_WM_ICON_NAME"),
            (180, "WM_HINTS"),
            (220, "_NET_WM_WINDOW_TYPE"),
            (220, "XdndAware"),
            (229, "WM_LOCALE_NAME"),
            (230, "_NET_WM_NAME"),
            (230, "_NET_WM_PID"),
            (230, "WM_CLIENT_MACHINE"),
            (240, "_NET_WM_DESKTOP"),
            (240, "_NET_WM_STATE"),
            (240, "WM_CLASS"),
            (240, "WM_NORMAL_HINTS"),
            (240, "WM_PROTOCOLS"),
            (240, "WM_STATE"),
        ];

        let mut rng = thread_rng();
        let event_type_dist = WeightedIndex::new(&[10, 10, 1])?;
        let window_properties_dist = WeightedIndex::new(window_properties.iter().map(|e| e.0))?;
        let app_id_dist = rand::distributions::Uniform::from(0..100);
        let data = (1..100000).map(|_| match event_type_dist.sample(&mut rng) {
            0 => {
                let mut properties = BTreeMap::new();
                for _i in 1..rand::distributions::Uniform::from(5..20).sample(&mut rng) {
                    let p = window_properties[window_properties_dist.sample(&mut rng)].1;
                    properties.insert(p.to_string(), "1".to_string());
                }
                EventData::OpenApplication {
                    id: app_id_dist.sample(&mut rng),
                    app_name: app_names.choose(&mut rng).unwrap().clone(),
                    app_type: app_types.choose(&mut rng).unwrap().clone(),
                    properties,
                }
            }
            1 => EventData::CloseApplication {
                id: app_id_dist.sample(&mut rng),
            },
            2 => EventData::Shutdown,
            _ => panic!("impossible"),
        });
        {
            let tx = db.transaction()?;
            let mut insert = tx.prepare("insert into events (timestamp, data) values (?, ?, ?)")?;
            for d in data {
                insert.execute(params![
                    chrono::Utc::now().to_rfc3339(),
                    serde_json::to_string_pretty(&d)?
                ])?;
            }
        }
        Ok(db)
    }

    #[test]
    fn sanity() -> anyhow::Result<()> {
        let _db = create_example_db()?;

        Ok(())
    }
    //
    // check that zstd_enable_transparent only creates one dictionary on the full table UPDATE

    //
    // check that `insert into events values ('a', 'b', 'c', 'd', 'e', 'f'), ('b', 'c', 'd', 'e', 'f', 'g');`
    // only creates one dictionary

    // check that decompress only creates one dictionary
}
