use crate::{util::*, *};
use anyhow::Context as AContext;

use env_logger::fmt::Timestamp;
use rusqlite::functions::Context;

use rusqlite::types::ToSqlOutput;
use rusqlite::types::Value;
use rusqlite::OptionalExtension;
use rusqlite::{named_params, params};
use std::time::{Duration, Instant};

static COMPACT: bool = true;
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
    5_000_000 / 3
}

/// This is the configuration of the transparent compression for one column of one table.
/// It is safe to change every property of this configuration at any time except for table and column, but data that is already compressed will not be recompressed with the new settings.
/// You can update the config e.g. using SQL: `update _zstd_configs set config = json_patch(config, '{"target_db_load": 1}');`
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
    /// how many bytes (approximately) to compress at once. By default tuned so at compression level 19 it locks the database for about 0.3s per step.
    #[serde(default = "def_incremental_compression_step_bytes")]
    pub incremental_compression_step_bytes: i64,
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

#[derive(Debug)]
enum SqliteAffinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}
/// determine affinity, algorithm described at https://www.sqlite.org/draft/datatype3.html#determination_of_column_affinity
fn get_column_affinity(declared_type: &str) -> SqliteAffinity {
    use SqliteAffinity::*;
    let typ = declared_type.to_ascii_lowercase();
    if typ.contains("int") {
        Integer
    } else if typ.contains("char") || typ.contains("clob") || typ.contains("text") {
        Text
    } else if typ.contains("blob") || typ.is_empty() {
        Blob
    } else if typ.contains("real") || typ.contains("floa") || typ.contains("doub") {
        Real
    } else {
        Numeric
    }
}

fn show_warnings(db: &Connection) -> anyhow::Result<()> {
    // warnings
    let journal_mode: String = db
        .query_row("pragma journal_mode;", params![], |r| r.get(0))
        .context("querying journal mode")?;
    if journal_mode != "wal" {
        log::warn!("Warning: It is recommended to set `pragma journal_mode=WAL;`");
    }
    let vacuum_mode: i32 = db
        .query_row("pragma auto_vacuum;", params![], |r| r.get(0))
        .context("querying vacuum mode")?;
    if vacuum_mode != 1 {
        log::warn!("Warning: It is recommended to set `pragma auto_vacuum=full;`");
    }
    let busy_timeout: i32 = db
        .query_row("pragma busy_timeout;", params![], |r| r.get(0))
        .context("querying busy timeout")?;
    if busy_timeout == 0 {
        log::warn!("Warning: It is recommended to set `pragma busy_timeout=2000;` or higher");
    }
    Ok(())
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
    let db = db
        .unchecked_transaction()
        .context("Could not start transaction")?;
    let table_name = &config.table;
    let new_table_name = format!("_{}_zstd", table_name);

    let table_type: String = db
        .query_row(
            "select `type` from sqlite_master where name=?",
            params![table_name],
            |r| r.get(0),
        )
        .context("Could not query sqlite master")?;
    if table_type != "table" {
        anyhow::bail!("{} is a {} not a table", table_name, table_type);
    }
    let columns_info: Vec<ColumnInfo> = db
        .prepare(&format_sqlite!(r#"pragma table_info({})"#, &table_name))?
        .query_map(params![], |row| {
            Ok(ColumnInfo {
                name: row.get("name")?,
                is_primary_key: row.get("pk")?,
                coltype: row.get("type")?,
            })
        })
        .context("Could not query table_info")?
        .collect::<Result<_, rusqlite::Error>>()?;

    show_warnings(&db)?;

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

    let affinity_is_text = match get_column_affinity(&to_compress_column.coltype) {
        SqliteAffinity::Blob => false,
        SqliteAffinity::Text => true,
        other => anyhow::bail!("the to-compress column has type {} which has affinity {:?}, but affinity must be text or blob. See https://www.sqlite.org/draft/datatype3.html#determination_of_column_affinity", to_compress_column.coltype, other)
    };
    let dict_id_column_name = format!("_{}_dict", to_compress_column.name);
    log::debug!("cols={:?}", columns_info);

    {
        let query = format!(
            "select ({}) as dict_chooser from {} limit 1",
            config.dict_chooser,
            escape_sqlite_identifier(&table_name)
        );
        // small sanity check of chooser statement
        db.query_row(&query, params![], |row| row.get::<_, String>(0))
            .optional()
            .context("Dict chooser expression does not seem to be valid")
            .with_context(|| format!("Tried to execute:\n{}", query))?;
    }
    {
        // can't use prepared statement at these positions
        let rename_query =
            format_sqlite!("alter table {} rename to {}", &table_name, &new_table_name);
        log::debug!("[run] {}", &rename_query);
        db.execute(&rename_query, params![])
            .context("Could not rename table")?;

        util::ensure_dicts_table_exists(&db)?;

        db.execute(
            "
            create table if not exists _zstd_configs (
                id integer primary key autoincrement,
                config json not null
            );",
            params![],
        )
        .context("Could not create _zstd_configs")?;

        db.execute(
            "insert into _zstd_configs (config) values (?)",
            params![config_str],
        )
        .context("Could not insert config")?;

        db.execute(
            &format_sqlite!(
                "alter table {} add column {} integer default null references _zstd_dicts(id)",
                &new_table_name,
                &dict_id_column_name
            ),
            params![],
        )
        .context("Could not add dictid column")?;

        db.execute(
            &format_sqlite!(
                "create index {} on {} ({})",
                &format!("{}_idx", &dict_id_column_name),
                &new_table_name,
                &dict_id_column_name
            ),
            params![],
        )
        .context("Could not create index on dictid")?;
    }

    {
        // create view
        let select_columns_escaped = columns_info
            .iter()
            .map(|c| {
                if column_name == &c.name {
                    format!(
                        // prepared statement parameters not allowed in view
                        "zstd_decompress_col({}, {}, {}, {}) as {0}",
                        &escape_sqlite_identifier(&column_name),
                        if affinity_is_text { 1 } else { 0 },
                        &escape_sqlite_identifier(&dict_id_column_name),
                        COMPACT
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
        db.execute(&createview_query, params![])
            .context("Could not create view")?;
    }

    {
        // insert trigger
        let insert_selection = columns_info
            .iter()
            .map(|c| {
                if column_name == &c.name {
                    format!(
                        // prepared statement parameters not allowed in view
                        "zstd_compress_col(new.{col}, {lvl}, (select id from _zstd_dicts where chooser_key=({chooser})), {compact}) as {col},
                        (select id from _zstd_dicts where chooser_key=({chooser})) as {dictcol}",
                        col=escape_sqlite_identifier(&column_name),
                        lvl=config.compression_level,
                        chooser=config.dict_chooser,
                        dictcol=escape_sqlite_identifier(&dict_id_column_name),
                        compact=COMPACT
                    )
                } else {
                    format_sqlite!("new.{}", &c.name)
                }
            })
            .collect::<Vec<String>>()
            .join(",\n");
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
        db.execute(&createtrigger_query, params![])
            .context("Could not create insert trigger")?;
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
        db.execute(&deletetrigger_query, params![])
            .context("could not create delete trigger")?;
    }
    {
        for col in &columns_info {
            let update = if col.name == to_compress_column.name {
                format!(
                    "{col} = zstd_compress_col(new.{col}, {lvl}, {dictcol}, {compact})",
                    col = escape_sqlite_identifier(&col.name),
                    lvl = config.compression_level,
                    dictcol = escape_sqlite_identifier(&dict_id_column_name),
                    compact = COMPACT
                )
            } else {
                format_sqlite!("{} = new.{}", &col.name, &col.name)
            };
            // update triggers
            let updatetrigger_query = format!(
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
            log::debug!("[run] {}", &updatetrigger_query);
            db.execute(&updatetrigger_query, params![])
                .with_context(|| format!("Could not create update of {} trigger", col.name))?;
        }
    }
    db.commit().context("Could not commit transaction")?;
    Ok(ToSqlOutput::Owned(Value::Text("Done!".to_string())))
}

#[derive(Debug)]
struct TodoInfo {
    dict_choice: Option<String>,
    count: i64,
    total_bytes: i64,
}

struct Args {
    end_limit: Instant,
    target_db_load: f32,
}
pub fn zstd_incremental_maintenance<'a>(ctx: &Context) -> Result<ToSqlOutput<'a>, anyhow::Error> {
    let arg_time_limit_seconds = 0;
    let arg_target_db_load = 1;
    let time_limit: f64 = ctx
        .get(arg_time_limit_seconds)
        .context("could not get time limit argument")?;
    let target_db_load: f32 = ctx
        .get(arg_target_db_load)
        .context("could not get target db load argument")?;
    if !(0.0..=1e100).contains(&time_limit) {
        anyhow::bail!("time too large");
    }
    let end_limit = Instant::now() + Duration::from_secs_f64(time_limit);
    let args = Args {
        end_limit,
        target_db_load,
    };
    let db = unsafe { ctx.get_connection()? };
    show_warnings(&db)?;
    let configs = db
        .prepare("select config from _zstd_configs")?
        .query_map(params![], |row| {
            serde_json::from_str(row.get_raw("config").as_str()?)
                .context("parsing config")
                .map_err(ah)
        })?
        .collect::<Result<Vec<TransparentCompressConfig>, _>>()?;
    for config in configs {
        match maintenance_for_config(&db, config, &args)? {
            MaintRet::TimeLimitReached => {
                log::info!(
                    "time limit of {:.1}s reached, stopping with more maintenance work pending",
                    time_limit
                );
                return Ok(1.into());
            }
            MaintRet::Completed => {}
        }
    }
    log::info!("All maintenance work completed!");
    Ok(0.into())
}

enum MaintRet {
    TimeLimitReached,
    Completed,
}

fn maintenance_for_config(
    db: &Connection,
    config: TransparentCompressConfig,
    args: &Args,
) -> anyhow::Result<MaintRet> {
    let compressed_tablename = escape_sqlite_identifier(&format!("_{}_zstd", config.table));
    let data_colname = escape_sqlite_identifier(&config.column);
    let dict_colname = escape_sqlite_identifier(&format!("_{}_dict", config.column));

    let todos = db
        .prepare(&format!(
            "select
            ({chooser}) as dict_choice,
            count(*) as count,
            sum(length({datacol})) as total_bytes
        from {tbl} where {dictcol} is null group by dict_choice",
            tbl = compressed_tablename,
            dictcol = dict_colname,
            datacol = data_colname,
            chooser = config.dict_chooser
        ))?
        .query_map(params![], |row| {
            Ok(TodoInfo {
                dict_choice: row.get("dict_choice")?,
                count: row.get("count")?,
                total_bytes: row.get("total_bytes")?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    let total_bytes_to_compress: i64 = todos
        .iter()
        .filter(|e| e.dict_choice.is_some())
        .map(|e| e.total_bytes)
        .sum();
    let mut rows_compressed_so_far: i64 = 0;
    let mut bytes_compressed_so_far: i64 = 0;
    let total_rows_to_compress: i64 = todos
        .iter()
        .filter(|e| e.dict_choice.is_some())
        .map(|e| e.count)
        .sum();
    log::info!(
        "{}.{}: Total {} rows ({}) to potentially compress.",
        config.table,
        config.column,
        total_rows_to_compress,
        pretty_bytes(total_bytes_to_compress)
    );
    for todo in todos.into_iter() {
        let rows_handled = maintenance_for_todo(
            db,
            &config,
            &todo,
            &data_colname,
            &compressed_tablename,
            &dict_colname,
            args,
        )?;
        rows_compressed_so_far += rows_handled;
        // estimate bytes compressed
        bytes_compressed_so_far +=
            ((rows_handled as f64 / todo.count as f64) * todo.total_bytes as f64) as i64;
        if rows_handled > 0 {
            log::info!(
                "Handled {} / {} rows  ({} / {})",
                rows_compressed_so_far,
                total_rows_to_compress,
                pretty_bytes(bytes_compressed_so_far),
                pretty_bytes(total_bytes_to_compress)
            );
        }
        if Instant::now() > args.end_limit {
            return Ok(MaintRet::TimeLimitReached);
        }
    }
    Ok(MaintRet::Completed)
}

fn maintenance_for_todo(
    db: &Connection,
    config: &TransparentCompressConfig,
    todo: &TodoInfo,
    data_colname: &str,
    compressed_tablename: &str,
    dict_colname: &str,
    args: &Args,
) -> anyhow::Result<i64> {
    let avg_sample_bytes = todo.total_bytes / todo.count;
    log::debug!(
        "looking at group={}, has {} rows with {} average size ({} total)",
        todo.dict_choice.as_deref().unwrap_or("[none]"),
        todo.count,
        pretty_bytes(avg_sample_bytes),
        pretty_bytes(todo.total_bytes)
    );

    let dict_choice = match &todo.dict_choice {
        None => {
            log::debug!("Skipping group, no dict chosen");
            return Ok(0);
        }
        Some(e) => e,
    };

    let dict_id: Option<i32> = db
        .query_row(
            "select id from _zstd_dicts where chooser_key = ?",
            params![dict_choice],
            |row| row.get("id"),
        )
        .optional()?;
    let (dict_id, dict_is_new) = match dict_id {
        Some(id) => {
            log::debug!(
                "Found existing dictionary id={} for key={}",
                id,
                dict_choice
            );
            (id, false)
        }
        None => {
            let dict_target_size = (todo.total_bytes as f32 * config.dict_size_ratio) as i64;

            if dict_target_size < config.min_dict_size_bytes_for_training {
                log::debug!(
                    "Dictionary for group '{}' would be smaller than minimum ({} * {:.3} = {} < {}), ignoring",
                    dict_choice,
                    pretty_bytes(todo.total_bytes),
                    config.dict_size_ratio,
                    pretty_bytes(dict_target_size),
                    pretty_bytes(config.min_dict_size_bytes_for_training)
                );
                return Ok(0);
            }
            let target_samples = (dict_target_size as f32 * config.train_dict_samples_ratio
                / avg_sample_bytes as f32) as i64; // use roughly 100x the size of the dictionary as data

            log::debug!(
                "Training dict for key {} of size {}",
                dict_choice,
                pretty_bytes(dict_target_size)
            );
            let id = db.query_row(&format!(
                "select zstd_train_dict_and_save({datacol}, ?, ?, ?) as dictid from {tbl} where {dictcol} is null and ? = ({chooser})", 
                datacol=data_colname,
                tbl=compressed_tablename,
                dictcol=dict_colname,
                chooser=config.dict_chooser
            ), params![dict_target_size, target_samples, dict_choice, dict_choice], |row| row.get("dictid"))?;
            (id, true)
        }
    };
    log::debug!(
        "Compressing {} samples with key {} and level {}",
        todo.count,
        dict_choice,
        config.compression_level
    );
    let mut total_updated: i64 = 0;
    let mut chunk_size = config.incremental_compression_step_bytes / avg_sample_bytes;
    if chunk_size < 1 {
        chunk_size = 1;
    }
    loop {
        let update_start = Instant::now();
        let updated = db.execute(
        &format!(
                "update {tbl} set {datacol} = zstd_compress_col({datacol}, :lvl, :dict, :compact), {dictcol} = :dict where rowid in (select rowid from {tbl} where {dictcol} is null and :dictchoice = ({chooser}) limit :chunksize)",
                tbl = compressed_tablename,
                datacol = data_colname,
                dictcol = dict_colname,
                chooser = config.dict_chooser
            ),
            named_params!{
                ":lvl": config.compression_level, 
                ":dict": dict_id,
                ":dictchoice": &dict_choice,
                ":chunksize": chunk_size,
                ":compact": COMPACT
            },
        ).context("compressing chunk")?;

        total_updated += updated as i64;
        log::debug!("Compressed {} / {}", total_updated, todo.count);
        if Instant::now() > args.end_limit {
            break;
        }
        let elapsed = update_start.elapsed();
        if elapsed.div_f32(args.target_db_load) > elapsed {
            let sleep_duration = elapsed.div_f32(args.target_db_load) - elapsed;
            if sleep_duration > Duration::from_millis(1) {
                log::debug!(
                    "Sleeping {}s to keep write load at {}",
                    sleep_duration.as_secs_f32(),
                    args.target_db_load
                );
                std::thread::sleep(sleep_duration);
            }
        }

        if updated == 0 {
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
    if dict_is_new {
        log::info!(
            "Compressed {} rows with dict_choice={} (dict_id={}). Total size of entries before: {}, afterwards: {}, (average: before={}, after={})",
            total_updated,
            dict_choice,
            dict_id,
            pretty_bytes(todo.total_bytes),
            pretty_bytes(total_size_after),
            pretty_bytes(avg_sample_bytes),
            pretty_bytes(total_size_after / total_count_after),
        );
    }
    Ok(total_updated)
}
#[cfg(test)]
mod tests {
    use super::add_functions::tests::create_example_db;
    use super::*;
    use pretty_assertions::assert_eq;
    use rand::prelude::SliceRandom;
    use rusqlite::params;
    use rusqlite::{Connection, Row};

    fn row_to_thong(r: &Row) -> anyhow::Result<Vec<Value>> {
        Ok((0..r.column_count())
            .map(|i| r.get_ref(i).map(|e| e.into()))
            .collect::<Result<_, _>>()?)
    }

    fn get_whole_table(db: &Connection, tbl_name: &str) -> anyhow::Result<Vec<Vec<Value>>> {
        let mut stmt = db.prepare(&format!("select * from {}", tbl_name))?;
        let q1: Vec<Vec<Value>> = stmt
            .query_map(params![], |e| row_to_thong(e).map_err(ah))?
            .collect::<Result<_, rusqlite::Error>>()?;
        Ok(q1)
    }

    fn check_table_rows_same(db1: &Connection, db2: &Connection) -> anyhow::Result<()> {
        let tbl1 = get_whole_table(db1, "events").context("Could not get whole table db 1")?;
        let tbl2 = get_whole_table(db2, "events").context("Could not get whole table db 2")?;
        assert_eq!(tbl1, tbl2);

        Ok(())
    }

    #[test]
    fn sanity() -> anyhow::Result<()> {
        let db1 = create_example_db(Some(123), 100)?;
        let db2 = create_example_db(Some(123), 100)?;

        check_table_rows_same(&db1, &db2)?;

        Ok(())
    }

    fn get_two_dbs() -> anyhow::Result<(Connection, Connection)> {
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "info");
        }
        env_logger::try_init().ok();

        let db1 = create_example_db(Some(123), 2000)?;
        let db2 = create_example_db(Some(123), 2000)?;

        db2.query_row(
            r#"select zstd_enable_transparent(?)"#,
            params![r#"{"table": "events", "column": "data", "compression_level": 3, "dict_chooser": "'1'"}"#],
            |_| Ok(())
        ).context("enable transparent")?;

        Ok((db1, db2))
    }
    #[test]
    fn enable_transparent() -> anyhow::Result<()> {
        let (db1, db2) = get_two_dbs()?;
        check_table_rows_same(&db1, &db2)?;

        Ok(())
    }

    fn get_rand_id(db: &Connection) -> anyhow::Result<i64> {
        db.query_row(
            "select id from events order by random() limit 1",
            params![],
            |r| r.get(0),
        )
        .context("Could not get random id")
    }

    fn insert(db: &Connection, id: i64, id2: i64) -> anyhow::Result<()> {
        let query = r#"insert into events (timestamp, data) values ('2020-12-20T00:00:00Z', '{"foo": "bar"}')"#;

        db.execute(query, params![])?;

        Ok(())
    }

    fn update_comp_col(db: &Connection, id: i64, id2: i64) -> anyhow::Result<()> {
        let updc = db.execute("update events set data='fooooooooooooooooooooooooooooooooooooooooooooobar' where id = ?", params![id]).context("updating compressed column")?;

        //assert_eq!(updc, 1);
        Ok(())
    }

    fn update_other_col(db: &Connection, id: i64, id2: i64) -> anyhow::Result<()> {
        let updc = db
            .execute(
                "update events set timestamp = '2020-02-01' where id = ?",
                params![id],
            )
            .context("updating other column")?;
        //assert_eq!(updc, 1);
        Ok(())
    }

    fn update_other_two_col(db: &Connection, id: i64, id2: i64) -> anyhow::Result<()> {
        //thread::rand
        delete_one(db, id2, id)?;
        let updc = db
            .execute(
                "update events set timestamp = '2020-02-01', id=? where id = ?",
                params![id2, id],
            )
            .context("updating other two column")?;
        //assert_eq!(updc, 1);
        Ok(())
    }

    fn update_comp_col_and_other_two_col(db: &Connection, id: i64, id2: i64) -> anyhow::Result<()> {
        //thread::rand
        delete_one(db, id2, id)?;
        let updc = db.execute("update events set timestamp = '2020-02-01', id=?, data='fooooooooooooooooooooooooooooooooooooooooooooobar' where id = ?", params![id2,id]).context("updating three column")?;
        //assert_eq!(updc, 1);
        Ok(())
    }

    fn update_two_rows(db: &Connection, id: i64, id2: i64) -> anyhow::Result<()> {
        //thread::rand
        let updc = db.execute("update events set timestamp = '2020-02-01', data='fooooooooooooooooooooooooooooooooooooooooooooobar' where id in (:a, :b)", 
        named_params! {":a": id, ":b": id2}).context("updating two rows")?;
        //assert_eq!(updc, 2);
        Ok(())
    }

    fn update_two_rows_by_compressed(db: &Connection, id: i64, id2: i64) -> anyhow::Result<()> {
        let updc = db
            .execute(
                "update events set data = 'testingxy' where id in (?, ?)",
                params![id, id2],
            )
            .context("updating")?;
        //assert_eq!(updc, 2);
        //thread::rand
        let updc = db
            .execute(
                "update events set timestamp='1234' where data = 'testingxy'",
                params![],
            )
            .context("updating where compressed")?;
        //assert_eq!(updc, 2);
        Ok(())
    }

    fn delete_one(db: &Connection, id: i64, id2: i64) -> anyhow::Result<()> {
        let updc = db
            .execute("delete from events where id = ?", params![id])
            .context("updating other column")?;
        //assert_eq!(updc, 1);
        Ok(())
    }

    fn delete_where_other(db: &Connection, id: i64, id2: i64) -> anyhow::Result<()> {
        let ts: String = db.query_row(
            "select timestamp from events where id = ?",
            params![id],
            |r| r.get(0),
        )?;
        let updc = db
            .execute("delete from events where timestamp = ?", params![ts])
            .context("updating other column")?;
        //assert_eq!(updc, 1);
        Ok(())
    }

    #[test]
    fn test_many() -> anyhow::Result<()> {
        let posses: Vec<&dyn Fn(&Connection, i64, i64) -> anyhow::Result<()>> = vec![
            &insert,
            &update_comp_col,
            &update_other_col,
            &update_other_two_col,
            &update_comp_col_and_other_two_col,
            &update_two_rows,
            &update_two_rows_by_compressed,
            &delete_one,
            &delete_where_other,
        ];

        let mut posses2 = vec![];
        for i in 0..100 {
            posses2.push(*posses.choose(&mut rand::thread_rng()).unwrap());
        }
        for compress_first in vec![false, true] {
            for operations in &[&posses2] {
                if compress_first {
                    let (db1, db2) = get_two_dbs().context("Could not create databases")?;
                    if compress_first {
                        let done: i64 = db2.query_row(
                            "select zstd_incremental_maintenance(9999999)",
                            params![],
                            |r| r.get(0),
                        )?;
                        assert_eq!(done, 0);
                        let uncompressed_count: i64 = db2
                            .query_row(
                                "select count(*) from _events_zstd where _data_dict is null",
                                params![],
                                |r| r.get(0),
                            )
                            .context("Could not query uncompressed count")?;
                        assert_eq!(uncompressed_count, 0);
                    }

                    for operation in *operations {
                        let id = get_rand_id(&db1)?;
                        let id2 = get_rand_id(&db2)?;
                        operation(&db1, id, id2)
                            .context("Could not run operation on uncompressed db")?;
                        operation(&db2, id, id2)
                            .context("Could not run operation on compressed db")?;
                    }

                    check_table_rows_same(&db1, &db2)?;
                }
            }
        }

        Ok(())
    }
}
