use crate::transparent::*;
use crate::util::*;
use crate::{dict_management::*, zstd_fns::ensure_dicts_table_exists};
use anyhow::Context as AContext;
use rand::Rng;
use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::ToSql;
use rusqlite::types::ToSqlOutput;
use rusqlite::types::{Value, ValueRef};
use rusqlite::{params, Connection};
use std::{io::Write, sync::Mutex};

pub struct ZstdTrainDictAggregate {
    /// if None, return trained dict, otherwise insert into _zstd_dicts table with chooser_key given as fourth arg and return id
    /// if false expects 3 args, if true expects 4 args
    pub return_save_id: bool,
}
pub struct ZstdTrainDictState {
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
