use crate::{dict_management::*, util::*, zstd_fns::*, *};
use anyhow::Context as AContext;
use rand::Rng;
use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::ToSql;
use rusqlite::types::ToSqlOutput;
use rusqlite::types::{Value, ValueRef};
use rusqlite::OptionalExtension;
use rusqlite::{named_params, params, Connection};
use std::{
    collections::HashMap,
    io::Write,
    sync::Mutex,
    time::{Duration, Instant},
};

#[derive(Debug)]
struct ColumnInfo {
    name: String,
    coltype: String,
    is_primary_key: bool,
}

fn def_min_dict_size() -> i64 {
    5000
}
fn def_dict_size_ratio() -> f32 {
    0.01
}
fn def_train_dict_samples_ratio() -> f32 {
    100.0
}
fn def_incremental_compression_step_bytes() -> i64 {
    // https://github.com/facebook/zstd/blob/dev/doc/images/CSpeed2.png
    // about 5MB/s at level 19
    5_000_000
}
fn def_target_db_load() -> f32 {
    0.3
}

/// This is the configuration of the transparent compression for one column of one table.
/// It is safe to change every property of this configuration at any time (except for table and column), but data that is already compressed will not be recompressed with the new settings.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct TransparentCompressConfig {
    /// the name of the table to which the transparent compression will be applied. It will be renamed to _tblname_zstd and replaced with a editable view.
    pub table: String,
    /// the name of the column
    pub column: String,
    /// The compression level. Valid levels are 1-19.
    /// Compression will be significantly slower when the level is increased, but decompression speed should stay about the same regardless of compression level.
    /// That means this is a tradeoff between INSERT vs SELECT performance.
    pub compression_level: i8,
    /// An SQL expression that chooses which dict to use or returns null if data should stay uncompressed for now
    /// Examples:
    ///
    /// * `strftime(created, '%Y-%m')`
    ///     this will cause every month of data to be compressed with its own dictionary.
    ///
    /// * `nullif(strftime(created, '%Y-%m'), strftime('now', '%Y-%m'))`
    ///
    ///     The same as above, but if the entry is from the current month it will stay uncompressed.
    ///     This is handy because it means that the dictionary for the month will only be created when the month is over
    ///     and can thus be optimized the most for the given data
    pub dict_chooser: String,
    #[serde(default = "def_min_dict_size")]
    /// if dictionary size would be smaller than this then no dict will be trained and if no dict exists the data will stay uncompressed
    pub min_dict_size_bytes_for_training: i64,
    #[serde(default = "def_dict_size_ratio")]
    /// The target size of the dictionary based on seen data. For example,
    /// For example if we see 10MB of data for a specific group, the dict will target a size of ratio * 10MB (default 0.01)
    pub dict_size_ratio: f32,
    /// for training, we find samples of this factor (default 100)
    /// the default of 100 and 0.01 means that by default the dict will be trained on all of the data
    #[serde(default = "def_train_dict_samples_ratio")]
    pub train_dict_samples_ratio: f32,
    /// how many bytes (approximately) to compress at once. By default tuned so at compression level 19 it locks the database for about 1s per step.
    #[serde(default = "def_incremental_compression_step_bytes")]
    pub incremental_compression_step_bytes: i64,
    /// During an incremental maintenance operation, target this ratio of DB load by sleeping between write operations.
    /// For example: if set to 0.5, after each write operation taking 2 seconds the maintenance function will sleep for 2 seconds so other processes have time to run write operations against the database.
    /// Note that this is only useful if you run the incremental maintenance function in a separate thread or process than your other logic.
    #[serde(default = "def_target_db_load")]
    pub target_db_load: f32,
}

fn pretty_bytes(bytes: i64) -> String {
    if bytes >= 1_000_000_000 {
        return format!("{:.2}GB", bytes as f64 / 1e9);
    } else if bytes >= 1_000_000 {
        return format!("{:.2}MB", bytes as f64 / 1e6);
    } else if bytes >= 1_000 {
        return format!("{:.2}kB", bytes as f64 / 1e3);
    } else {
        return format!("{}B", bytes);
    }
}

///
/// enables transparent row-level compression for a table with the following steps:
///
/// 1. renames tablename to _tablename_zstd
/// 2. trains a zstd dictionary on the column by sampling the existing data in the table
/// 3. creates a view called tablename that mirrors _tablename_zstd except it decompresses the compressed column on the fly
/// 4. creates INSERT, UPDATE and DELETE triggers on the view so they affect the backing table instead
///
/// Warning: this function assumes trusted input, it is not sql injection safe!
pub fn zstd_enable_transparent<'a>(ctx: &Context) -> anyhow::Result<ToSqlOutput<'a>> {
    let arg_config = 0;

    let config_str: String = ctx.get(arg_config)?;
    let config: TransparentCompressConfig = serde_json::from_str(&config_str)
        .with_context(|| format!("parsing json config '{}'", config_str))?;
    let db = &mut unsafe { ctx.get_connection()? };
    let db = db.unchecked_transaction()?;
    let table_name = &config.table;
    let new_table_name = format!("_{}_zstd", table_name);
    let columns_info: Vec<ColumnInfo> = db
        .prepare(&format_sqlite!(r#"pragma table_info({})"#, &table_name))?
        .query_map(params![], |row| {
            Ok(ColumnInfo {
                name: row.get("name")?,
                is_primary_key: row.get("pk")?,
                coltype: row.get("type")?,
            })
        })?
        .collect::<Result<_, rusqlite::Error>>()?;

    let journal_mode: String = db
        .query_row("pragma journal_mode;", params![], |r| r.get(0))
        .context("querying journal mode")?;
    if journal_mode != "wal" {
        log::warn!("Warning: It is recommended to set `pragma journal_mode=WAL;`");
    }

    // primary key columns. these will be used to index the table in the modifying triggers
    let primary_key_columns: Vec<&ColumnInfo> =
        columns_info.iter().filter(|e| e.is_primary_key).collect();

    if columns_info.is_empty() {
        anyhow::bail!("Table {} does not exist", table_name);
    }
    if primary_key_columns.is_empty() {
        anyhow::bail!(
            "Table {} does not have a primary key, sqlite-zstd only works on tables with primary keys",
            table_name
        );
    }
    let column_name = &config.column;

    let to_compress_column = columns_info
        .iter()
        .find(|e| &e.name == column_name)
        .with_context(|| format!("Column {} does not exist in {}", column_name, table_name))?;
    if to_compress_column.is_primary_key {
        anyhow::bail!("Can't compress column {} since it is part of primary key (this could probably be supported, but currently isn't)", column_name);
    }
    let dict_id_column_name = format!("_{}_dict", to_compress_column.name);
    log::debug!("cols={:?}", columns_info);

    {
        let query = format!(
            "select ({}) from {} limit 1",
            config.dict_chooser,
            escape_sqlite_identifier(&table_name)
        );
        // small sanity check of chooser statement
        db.query_row(&query, params![], |_row| Ok(()))
            .context("Dict chooser expression does not seem to be valid")
            .with_context(|| format!("Tried to execute:\n{}", query))?;
    }
    {
        // can't use prepared statement at these positions
        let rename_query =
            format_sqlite!("alter table {} rename to {}", &table_name, &new_table_name);
        log::debug!("[run] {}", &rename_query);
        db.execute(&rename_query, params![])?;

        ensure_dicts_table_exists(&db)?;

        db.execute(
            "
            create table if not exists _zstd_configs (
                id integer primary key autoincrement,
                config json not null
            );",
            params![],
        )?;

        db.execute(
            "insert into _zstd_configs (config) values (?)",
            params![config_str],
        )?;

        db.execute(
            &format_sqlite!(
                "alter table {} add column {} integer default null references _zstd_dicts(id)",
                &new_table_name,
                &dict_id_column_name
            ),
            params![],
        )?;

        db.execute(
            &format_sqlite!(
                "create index {} on {} ({})",
                &format!("{}_idx", &dict_id_column_name),
                &new_table_name,
                &dict_id_column_name
            ),
            params![],
        )?;
    }

    /*let dict_size = 1_000_000; // 100kB is the default
    let group_by = "null";
    let compression_level = 19; // 0 = default = 3
    let dict_rowid = {
        // we use sample_count = select dict_size * 100 / avg(length(data))
        // because the zstd docs recommend using around 100x the target dictionary size of data to train the dictionary
        let train_query = format_sqlite!("
            insert into _zstd_dicts (meta,dict) values (:meta,
                (select zstd_train_dict({0}, :dict_size, (select :dict_size * 100 / avg(length({0})) as sample_count from {1}))
                    as dict from {1})
            );", &new_column_name, &new_table_name);
        log::debug!("[run] {}", &train_query);
        db.execute(
            &train_query,
            named_params! {
                ":meta": serde_json::to_string(&json!({
                    "tbl": table_name,
                    "col": new_column_name,
                    "group_by": group_by,
                    "dict_size": dict_size,
                    "created": chrono::Utc::now().to_rfc3339()
                })).map_err(|e| UFE(Box::new(e)))?,
                ":dict_size": dict_size
            },
        )?;
        db.last_insert_rowid()
    };
    {
        let compress_query = format_sqlite!(
            "update {} set {1} = zstd_compress({1}, ?, ?)",
            &new_table_name,
            &column_name
        );
        log::debug!("[run] {}", compress_query);
        let updated = db.execute(&compress_query, params![compression_level, dict_rowid])?;
        log::debug!(
            "compressed {} rows of {}.{}",
            updated, table_name, column_name
        );
    }*/

    {
        let select_columns_escaped = columns_info
            .iter()
            .map(|c| {
                if column_name == &c.name {
                    format_sqlite!(
                        // prepared statement parameters not allowed in view
                        "zstd_decompress_col({}, {}) as {0}",
                        &column_name,
                        &dict_id_column_name
                    )
                } else {
                    format_sqlite!("{}", &c.name)
                }
            })
            .collect::<Vec<String>>()
            .join(", ");
        let createview_query = format!(
            r#"
            create view {} as
                select {}
                from {}
            "#,
            escape_sqlite_identifier(&table_name),
            select_columns_escaped,
            escape_sqlite_identifier(&new_table_name)
        );
        log::debug!("[run] {}", &createview_query);
        db.execute(&createview_query, params![])?;
    }

    {
        // insert trigger
        let insert_selection = columns_info
            .iter()
            .map(|c| {
                if column_name == &c.name {
                    format!(
                        // prepared statement parameters not allowed in view
                        "zstd_compress_col(new.{}, {}, (select dict from _zstd_dicts where chooser_key=({}))) as {0}",
                        escape_sqlite_identifier(&column_name),
                        config.compression_level,
                        config.dict_chooser
                    )
                } else {
                    format_sqlite!("new.{}", &c.name)
                }
            })
            .collect::<Vec<String>>()
            .join(", ");
        let createtrigger_query = format!(
            "
            create trigger {}
                instead of insert on {}
                for each row
                begin
                    insert into {} select {};
                end;
            ",
            escape_sqlite_identifier(&format!("{}_insert_trigger", table_name)),
            escape_sqlite_identifier(&table_name),
            escape_sqlite_identifier(&new_table_name),
            insert_selection
        );
        log::debug!("[run] {}", &createtrigger_query);
        db.execute(&createtrigger_query, params![])?;
    }
    // a WHERE statement that selects a row based on the primary key
    let primary_key_condition = primary_key_columns
        .iter()
        .map(|c| format_sqlite!("old.{0} = {0}", &c.name))
        .collect::<Vec<String>>()
        .join(" and ");
    {
        // add delete trigger
        let deletetrigger_query = format!(
            "
            create trigger {trg_name}
                instead of delete on {view}
                for each row
                begin
                    delete from {backing_table} where {primary_key_condition};
                end;
            ",
            trg_name = escape_sqlite_identifier(&format!("{}_delete_trigger", table_name)),
            view = escape_sqlite_identifier(&table_name),
            backing_table = escape_sqlite_identifier(&new_table_name),
            primary_key_condition = primary_key_condition
        );
        log::debug!("[run] {}", &deletetrigger_query);
        db.execute(&deletetrigger_query, params![])?;
    }
    {
        for col in &columns_info {
            let update = if col.name == to_compress_column.name {
                format_sqlite!(
                    "{} = zstd_compress_col(new.{}, {})",
                    &col.name,
                    &col.name,
                    &dict_id_column_name
                )
            } else {
                format_sqlite!("{} = new.{}", &col.name, &col.name)
            };
            // update triggers
            let deletetrigger_query = format!(
                "
                create trigger {trg_name}
                    instead of update of {upd_col} on {view_name}
                    for each row
                    begin
                        update {backing_table} set {update} where {primary_key_condition};
                    end;
                ",
                trg_name = escape_sqlite_identifier(&format!(
                    "{}_update_{}_trigger",
                    table_name, col.name
                )),
                view_name = escape_sqlite_identifier(&table_name),
                backing_table = escape_sqlite_identifier(&new_table_name),
                upd_col = escape_sqlite_identifier(&col.name),
                update = update,
                primary_key_condition = primary_key_condition
            );
            log::debug!("[run] {}", &deletetrigger_query);
            db.execute(&deletetrigger_query, params![])?;
        }
    }
    db.commit()?;
    log::debug!("consider running pragma vacuum to clean up old data");
    Ok(ToSqlOutput::Owned(Value::Text("Done!".to_string())))
}

#[derive(Debug)]
struct TodoInfo {
    dict_choice: String,
    count: i64,
    total_bytes: i64,
}
pub fn zstd_incremental_maintenance<'a>(ctx: &Context) -> Result<ToSqlOutput<'a>, anyhow::Error> {
    let arg_time_limit_seconds = 0;
    let time_limit: f64 = ctx
        .get(arg_time_limit_seconds)
        .context("could not get time limit argument")?;
    if time_limit > 1e100 || time_limit < 0.0 {
        anyhow::bail!("time too large");
    }
    let end_limit = Instant::now() + Duration::from_secs_f64(time_limit);
    let db = unsafe { ctx.get_connection()? };
    let configs = db
        .prepare("select config from _zstd_configs")?
        .query_map(params![], |row| {
            serde_json::from_str(row.get_raw("config").as_str()?)
                .context("parsing config")
                .map_err(ah)
        })?
        .collect::<Result<Vec<TransparentCompressConfig>, _>>()?;
    for config in configs {
        log::debug!("looking at {}.{}", config.table, config.column);

        let compressed_tablename = escape_sqlite_identifier(&format!("_{}_zstd", config.table));
        let data_colname = escape_sqlite_identifier(&config.column);
        let dict_colname = escape_sqlite_identifier(&format!("_{}_dict", config.column));

        let todos = db.prepare(&format!(
            "select ({chooser}) as dict_choice, count(*) as count, sum(length({datacol})) as total_bytes from {tbl} where {dictcol} is null group by dict_choice",
            tbl = compressed_tablename,
            dictcol = dict_colname,
            datacol = data_colname,
            chooser = config.dict_chooser
        ))?.query_map(params![], |row| Ok(
            TodoInfo {dict_choice: row.get("dict_choice")?, count: row.get("count")?, total_bytes: row.get("total_bytes")?
        }))?.collect::<Result<Vec<_>, _>>()?;
        for todo in todos {
            let avg_sample_bytes = todo.total_bytes / todo.count;
            log::debug!(
                "looking at group={}, has {} rows with {} average size ({} total)",
                todo.dict_choice,
                todo.count,
                pretty_bytes(avg_sample_bytes),
                pretty_bytes(todo.total_bytes)
            );

            let dict_id: Option<i32> = db
                .query_row(
                    "select id from _zstd_dicts where chooser_key = ?",
                    params![todo.dict_choice],
                    |row| row.get("id"),
                )
                .optional()?;
            let dict_id = match dict_id {
                Some(id) => {
                    log::debug!(
                        "Found existing dictionary id={} for key={}",
                        id,
                        todo.dict_choice
                    );
                    id
                }
                None => {
                    let dict_target_size =
                        (todo.total_bytes as f32 * config.dict_size_ratio) as i64;

                    if dict_target_size < config.min_dict_size_bytes_for_training {
                        log::debug!(
                            "Dictionary for group {} would be smaller than minimum ({} * {:.3} = {} < {}), ignoring",
                            pretty_bytes(todo.total_bytes),
                            config.dict_size_ratio,
                            todo.dict_choice,
                            pretty_bytes(dict_target_size),
                            pretty_bytes(config.min_dict_size_bytes_for_training)
                        );
                        continue;
                    }
                    let target_samples = (dict_target_size as f32 * config.train_dict_samples_ratio
                        / avg_sample_bytes as f32) as i64; // use roughly 100x the size of the dictionary as data

                    log::debug!(
                        "Training dict for key {} of size {}",
                        todo.dict_choice,
                        pretty_bytes(dict_target_size)
                    );
                    db.query_row(&format!(
                        "select zstd_train_dict_and_save({datacol}, ?, ?, ?) as dictid from {tbl} where {dictcol} is null and ? = ({chooser})", 
                        datacol=data_colname,
                        tbl=compressed_tablename,
                        dictcol=dict_colname,
                        chooser=config.dict_chooser
                    ), params![dict_target_size, target_samples, todo.dict_choice, todo.dict_choice], |row| row.get("dictid"))?
                }
            };
            log::debug!(
                "Compressing {} samples with key {} and level {}",
                todo.count,
                todo.dict_choice,
                config.compression_level
            );
            let mut total_updated = 0;
            let mut chunk_size = config.incremental_compression_step_bytes / avg_sample_bytes;
            if chunk_size < 1 {
                chunk_size = 1;
            }
            loop {
                let updated = db.execute(
                &format!(
                        "update {tbl} set {datacol} = zstd_compress({datacol}, :lvl, :dict), {dictcol} = :dict where rowid in (select rowid from {tbl} where {dictcol} is null and :dictchoice = ({chooser}) limit :chunksize)",
                        tbl = compressed_tablename,
                        datacol = data_colname,
                        dictcol = dict_colname,
                        chooser = config.dict_chooser
                    ),
                    named_params!{
                        ":lvl": config.compression_level, 
                        ":dict": dict_id,
                        ":dictchoice": todo.dict_choice,
                        ":chunksize": chunk_size
                    },
                ).context("compressing chunk")?;

                if updated == 0 {
                    break;
                }
                total_updated += updated;
                log::debug!("Compressed {} / {}", total_updated, todo.count);
                if Instant::now() > end_limit {
                    break;
                }
            }

            let (total_size_after, total_count_after): (i64, i64) = db.query_row(
                &format!(
                    "select sum(length({datacol})), count(*) from {tbl} where {dictcol} = ?",
                    tbl = compressed_tablename,
                    datacol = data_colname,
                    dictcol = dict_colname
                ),
                params![dict_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            log::debug!(
                "total size of entries with dictid={} afterwards: {}, average={} (before={})",
                dict_id,
                pretty_bytes(total_size_after / 1000,),
                pretty_bytes(total_size_after / total_count_after,),
                pretty_bytes(avg_sample_bytes),
            );
            if Instant::now() > end_limit {
                log::debug!(
                    "time limit of {:.1}s reached, stopping with more maintenance work pending",
                    time_limit
                );
                return Ok(1.into());
            }
        }
    }
    log::debug!("maintenence complete!");
    Ok(0.into())
}
#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::{assert_eq, assert_ne};
    use rusqlite::{Connection, Row};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    fn RowToThong(r: &Row) -> Vec<Value> {
        (0..r.column_count()).map(|i| r.get_raw(i).into()).collect()
    }

    fn get_whole_table(db: &Connection, tbl_name: &str) -> anyhow::Result<Vec<Vec<Value>>> {
        let mut stmt = db.prepare(&format!("select * from {}", tbl_name))?;
        let q1: Vec<Vec<Value>> = stmt
            .query_map(params![], |e| Ok(RowToThong(e)))?
            .collect::<Result<_, rusqlite::Error>>()?;
        Ok(q1)
    }

    fn check_table_rows_same(db1: &Connection, db2: &Connection) -> anyhow::Result<()> {
        let tbl1 = get_whole_table(db1, "events")?;
        let tbl2 = get_whole_table(db2, "events")?;
        assert_eq!(tbl1, tbl2);

        Ok(())
    }

    #[test]
    fn sanity() -> anyhow::Result<()> {
        let db1 = super::zstd_fns::tests::create_example_db(Some(123), 100)?;
        let db2 = super::zstd_fns::tests::create_example_db(Some(123), 100)?;

        check_table_rows_same(&db1, &db2)?;

        Ok(())
    }

    #[test]
    fn enable_transparent() -> anyhow::Result<()> {
        let db1 = super::zstd_fns::tests::create_example_db(Some(123), 100)?;
        let db2 = super::zstd_fns::tests::create_example_db(Some(123), 100)?;

        db2.query_row(
            r#"select zstd_enable_transparent('{"table": "events", "column": "data", "compression_level": 3, "dict_chooser": "1"}')"#,
            params![],
            |_| Ok(())
        ).context("enable transparent")?;
        check_table_rows_same(&db1, &db2)?;

        Ok(())
    }
}
