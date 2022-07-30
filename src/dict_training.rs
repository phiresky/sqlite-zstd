use crate::transparent::pretty_bytes;
use crate::util::*;
use anyhow::Context as AContext;
use rand::Rng;
use rusqlite::functions::Context;

use rusqlite::params;
use rusqlite::types::{Value, ValueRef};

pub struct ZstdTrainDictAggregate {
    /// if None, return trained dict, otherwise insert into _zstd_dicts table with chooser_key given as fourth arg and return id
    /// if false expects 3 args, if true expects 4 args
    pub return_save_id: bool,
}
pub struct ZstdTrainDictState {
    reservoir: Vec<Vec<u8>>,
    wanted_item_count: usize,
    total_count: usize,
    wanted_dict_size: usize,
    chooser_key: Option<Option<String>>,
}

impl rusqlite::functions::Aggregate<ZstdTrainDictState, Value> for ZstdTrainDictAggregate {
    fn init(&self, ctx: &mut Context) -> rusqlite::Result<ZstdTrainDictState> {
        let arg_dict_size_bytes = 1;
        let arg_sample_count = 2;
        let arg_chooser_key = 3;
        let wanted_item_count = ctx.get::<f64>(arg_sample_count)? as usize;
        log::debug!("sampling {} values", wanted_item_count);
        Ok(ZstdTrainDictState {
            reservoir: vec![],
            wanted_item_count,
            wanted_dict_size: ctx.get::<i64>(arg_dict_size_bytes)? as usize,
            total_count: 0,
            chooser_key: if self.return_save_id {
                Some(ctx.get(arg_chooser_key)?)
            } else {
                None
            },
        })
    }
    fn step(&self, ctx: &mut Context, state: &mut ZstdTrainDictState) -> rusqlite::Result<()> {
        let arg_sample = 0;

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
        let j = rand::thread_rng().gen_range(0..i);
        if j < k {
            state.reservoir[j] = Vec::from(cur);
        }
        Ok(())
    }

    fn finalize(
        &self,
        ctx: &mut Context,
        state: Option<ZstdTrainDictState>,
    ) -> rusqlite::Result<Value> {
        let state =
            state.ok_or_else(|| ah(anyhow::anyhow!("tried to train zstd dict on zero rows")))?;
        log::debug!(
            "training dict of max size {}kB with {} samples of total size {}kB (of {} samples seen)",
            state.wanted_dict_size / 1000,
            state.reservoir.len(),
            state.reservoir.iter().map(|x| x.len()).sum::<usize>() / 1000,
            state.total_count
        );
        let dict = zstd::dict::from_samples(&state.reservoir, state.wanted_dict_size)
            .context("Training dictionary failed")
            .map_err(ah)?;
        log::debug!(
            "resulting dict has size {}",
            pretty_bytes(dict.len() as i64)
        );
        if let Some(key) = state.chooser_key {
            let db = unsafe { ctx.get_connection()? };
            ensure_dicts_table_exists(&db)?;
            db.execute(
                "insert into _zstd_dicts (chooser_key,dict) values (?, ?);",
                params![key, dict],
            )?;
            let id = db.last_insert_rowid();
            log::debug!(
                "inserted dict into _zstd_dicts with key {}, id {}",
                key.as_deref().unwrap_or("null"),
                id
            );
            Ok(Value::Integer(id))
        } else {
            Ok(Value::Blob(dict))
        }
    }
}
