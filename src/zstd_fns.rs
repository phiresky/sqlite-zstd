use rand::Rng;
use rusqlite::functions::Context;
use rusqlite::functions::FunctionFlags;
use rusqlite::params;
use rusqlite::types::ToSql;
use rusqlite::types::ToSqlOutput;
use rusqlite::types::{Value, ValueRef};
use rusqlite::Error::UserFunctionError as UFE;
use std::borrow::Cow;
use std::convert::TryInto;
use std::fs::File;
use std::io::Read;
use std::io::Write;

/*fn to_rusqlite<E>(e: Box<E>) -> rusqlite::Error
where
    E: std::error::Error + std::marker::Send + std::marker::Sync,
{
    rusqlite::Error::UserFunctionError(e)
}*/

/*fn to_rusqlite<'e>(
    e: impl std::error::Error + std::marker::Send + std::marker::Sync + 'e,
) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(e))
}*/

struct ZstdTrainDictAggregate;
struct ZstdTrainDictState {
    reservoir: Vec<Vec<u8>>,
    wanted_item_count: usize,
    total_count: usize,
    wanted_dict_size: usize,
}
impl rusqlite::functions::Aggregate<Option<ZstdTrainDictState>, Value> for ZstdTrainDictAggregate {
    fn init(&self) -> Option<ZstdTrainDictState> {
        None
    }
    fn step(
        &self,
        ctx: &mut Context,
        state: &mut Option<ZstdTrainDictState>,
    ) -> rusqlite::Result<()> {
        if let None = state {
            std::mem::replace(
                state,
                Some(ZstdTrainDictState {
                    reservoir: vec![],
                    wanted_item_count: ctx.get::<f64>(2)? as usize,
                    wanted_dict_size: ctx.get::<i64>(1)? as usize,
                    total_count: 0,
                }),
            );
        }
        let mut state = state.as_mut().unwrap();
        let cur = match ctx.get_raw(0) {
            ValueRef::Blob(b) => b,
            ValueRef::Text(b) => b,
            ValueRef::Real(f) => return Ok(()),
            ValueRef::Integer(i) => return Ok(()),
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
        if let Some(state) = state.flatten() {
            eprintln!(
                "training dict of size {}kB with {} samples",
                state.wanted_dict_size / 1000,
                state.reservoir.len()
            );
            let dict = zstd::dict::from_samples(&state.reservoir, state.wanted_dict_size)
                .map_err(|e| UFE(Box::new(e)))?;
            Ok(Value::Blob(dict))
        } else {
            Ok(Value::Null)
        }
    }
}

fn zstd_compress(ctx: &Context) -> Result<Box<dyn ToSql>, rusqlite::Error> {
    let (is_blob, input_value) = match ctx.get_raw(0) {
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
        ctx.get::<i32>(1)?
    };
    let dict_raw = if ctx.len() < 3 {
        // no third argument -> no dict
        None
    } else {
        ctx.get::<Option<Vec<u8>>>(2)?
    };
    use zstd::dict::EncoderDictionary;
    let dict: Option<EncoderDictionary> =
        dict_raw.as_ref().map(|e| EncoderDictionary::new(e, level));
    /*let dict: Option<&EncoderDictionary> = {
        // cache dict instance to auxiliary data so its not created every time
        // i'm too stupid for this
        let dict = match ctx.get_aux::<Option<EncoderDictionary>>(2)? {
            Some(d) => d.as_ref(),
            None => match ctx.get::<Option<Vec<u8>>>(2)? {
                Some(d) => {
                    let dict = EncoderDictionary::new(d.as_ref(), level);
                    ctx.set_aux(2, Some(dict));
                    ctx.get_aux::<Option<EncoderDictionary>>(2)?
                        .unwrap()
                        .as_ref()
                }
                None => {
                    ctx.set_aux(2, Some(Option::<EncoderDictionary>::None));
                    None
                }
            },
        };
        dict
    };*/
    let is_blob: &[u8] = if is_blob { b"b" } else { b"s" };
    let res = {
        let out = Vec::new();
        let mut encoder = match dict {
            Some(dict) => zstd::stream::write::Encoder::with_prepared_dictionary(out, &dict),
            None => zstd::stream::write::Encoder::new(out, level),
        }
        .map_err(|e| UFE(Box::new(e)))?;
        encoder
            .write_all(input_value)
            .map_err(|e| UFE(Box::new(e)))?;
        encoder.write_all(is_blob).map_err(|e| UFE(Box::new(e)))?;
        encoder.finish()
    };
    // let dictionary
    Ok(Box::new(res.map_err(|e| UFE(Box::new(e)))?))
}

fn zstd_decompress<'a>(ctx: &Context) -> Result<ToSqlOutput<'a>, rusqlite::Error> {
    let input_value = match ctx.get_raw(0) {
        ValueRef::Blob(b) => b,
        ValueRef::Text(_b) => {
            return Err(UFE(anyhow::anyhow!(
                "got string, but zstd compressed data is always blob"
            )
            .into()))
        }
        ValueRef::Real(f) => return Ok(ToSqlOutput::Owned(Value::Real(f))),
        ValueRef::Integer(i) => return Ok(ToSqlOutput::Owned(Value::Integer(i))),
        ValueRef::Null => return Ok(ToSqlOutput::Owned(Value::Null)),
    };

    let dict_raw = if ctx.len() < 2 {
        // no third argument -> no dict
        None
    } else {
        ctx.get::<Option<Vec<u8>>>(1)?
    };

    let mut vec = {
        let out = Vec::new();
        let mut decoder = match dict_raw {
            Some(dict) => zstd::stream::write::Decoder::with_dictionary(out, &dict),
            None => zstd::stream::write::Decoder::new(out),
        }
        .map_err(|_e| UFE(anyhow::anyhow!("dict load dosnt work").into()))?;
        decoder
            .write_all(input_value)
            .map_err(|e| UFE(Box::new(e)))?;
        decoder.flush().map_err(|e| UFE(Box::new(e)))?;
        decoder.into_inner()
    };

    let is_blob = vec.pop().unwrap();
    if is_blob == b'b' {
        Ok(ToSqlOutput::Owned(Value::Blob(vec)))
    } else {
        Ok(ToSqlOutput::Owned(Value::Text(
            unsafe { String::from_utf8_unchecked(vec) }, // converted right back to &u8 in https://docs.rs/rusqlite/0.21.0/src/rusqlite/types/value_ref.rs.html#107
        )))
    }
    // let dictionary
}

/*fn context_to_db(ctx: &Context) -> Connection {
    let handle = rusqlite::ffi::sqlite3_context_db_handle(ctx.ctx);
}*/

fn debug_row(r: &rusqlite::Row) {
    println!("debugrow");
    let cols = r.column_names();
    for (i, name) in cols.iter().enumerate() {
        print!("{}={}", name, format_blob(r.get_raw(i)))
    }
    println!("");
}

// to dumb for recursive macros
macro_rules! format_sqlite {
    ($x:expr) => {
        format!($x)
    };
    ($x:expr, $y:expr) => {
        format!($x, escape_sqlite_identifier($y))
    };
    ($x:expr, $y:expr, $z:expr) => {
        format!(
            $x,
            escape_sqlite_identifier($y),
            escape_sqlite_identifier($z)
        )
    };
    ($x:expr, $y:expr, $z:expr, $w:expr) => {
        format!(
            $x,
            escape_sqlite_identifier($y),
            escape_sqlite_identifier($z),
            escape_sqlite_identifier($w)
        )
    };
}

fn zstd_transparently<'a>(ctx: &Context) -> Result<ToSqlOutput<'a>, rusqlite::Error> {
    let table_name: String = ctx.get(0)?;
    let new_table_name = format!("_{}_zstd", table_name);
    let column_name: String = ctx.get(1)?;
    let db = ctx.get_connection()?;

    let columns: Vec<String> = db
        .prepare(&format_sqlite!(r#"pragma table_info({})"#, &table_name))?
        .query_map(params![], |row| row.get::<&str, String>("name"))?
        .collect::<Result<_, rusqlite::Error>>()?;
    if !columns.contains(&column_name) {
        return Err(UFE(anyhow::anyhow!(
            "Column {} does not exist in {}",
            column_name,
            table_name
        )
        .into()));
    }
    println!("cols={:?}", columns);
    // can't use prepared statement at these positions
    let rename = format_sqlite!(
        r#"alter table {} rename to {}"#,
        &table_name,
        &new_table_name
    );
    eprintln!("[run] {}", &rename);
    /*for row in */
    match db.prepare(&rename)?.query(params![])?.next()? {
        None => {}
        Some(r) => {
            debug_row(r);
            return Err(UFE(anyhow::anyhow!(
                "unexpected row returned from alter table"
            )
            .into()));
        }
    }

    db.execute(
        "
        create table if not exists _zstd_dicts (
            tbl text not null,
            col text not null,
            group_by text,
            dict blob not null,
            primary key (tbl, col)
        );
        ",
        params![],
    )?;
    let select_columns_escaped = columns.iter().map(|c| {
        if &column_name == c {
            format!("zstd_decompress({}, (select dict from _zstd_dicts where tbl={} and col={} and group_by={})) as {0}",
                escape_sqlite_identifier(&column_name),
                escape_sqlite_string(&table_name),
                escape_sqlite_string(&column_name),
                "null"
            )
        } else {
            format_sqlite!("{}", c)
        }
    }).collect::<Vec<String>>().join(", ");
    let createview = format!(
        r#"
        create view {} as
        select {}
        from {}
    "#,
        escape_sqlite_identifier(&table_name),
        select_columns_escaped,
        escape_sqlite_identifier(&new_table_name)
    );
    eprintln!("[run] {}", &createview);
    db.execute(&createview, params![])?;
    Ok(ToSqlOutput::Owned(Value::Text("Done!".to_string())))
}

pub fn add_functions(db: rusqlite::Connection) -> anyhow::Result<()> {
    let nondeterministic = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY;
    let deterministic = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    // zstd_compress(data: text|blob, level: int = 3, dictionary: blob | null = null)
    db.create_scalar_function("zstd_compress", 1, deterministic, zstd_compress)?;
    db.create_scalar_function("zstd_compress", 2, deterministic, zstd_compress)?;
    db.create_scalar_function("zstd_compress", 3, deterministic, zstd_compress)?;
    // zstd_decompress(data: text|blob, dictionary: blob | null = null)
    db.create_scalar_function("zstd_decompress", 1, deterministic, zstd_decompress)?;
    db.create_scalar_function("zstd_decompress", 2, deterministic, zstd_decompress)?;
    db.create_aggregate_function(
        "zstd_train_dict",
        3,
        nondeterministic,
        ZstdTrainDictAggregate,
    )?;
    db.create_scalar_function(
        "zstd_transparently",
        2,
        nondeterministic,
        zstd_transparently,
    )?;

    // tests
    /*let x: String = db.query_row(
        "select zstd_decompress(zstd_compress('test test test test test test test test test', 19, null))",
        params![],
        |row| row.get(0),
    )?;

    println!("result = {}", &x);*/

    /*db.execute_batch(
        "
        create table if not exists _zstd_dicts (
            name text primary key not null,
            dict blob not null
        );
        insert or ignore into _zstd_dicts values ('data',
            (select zstd_train_dict(data, 100000, (select 100000 * 100 / avg(length(data)) as sample_count from events))
                as dict from events)
        );
        update events set data = zstd_compress(data, 3, (select dict from _zstd_dicts where name = 'data'));
        alter table events rename to events_compressed;
        "
    )?;*/
    /*db.execute("drop view if exists events", params![])?;
    db.execute("create view if not exists events as
    select id, timestamp, data_type, sampler, sampler_sequence_id, zstd_decompress(data, (select dict from _zstd_dicts where name='data')) as data from events_compressed", params![])?;
    let mut stmt =
        db.prepare("explain query plan select * from events where timestamp > '2020' limit 10")?;
    let col_names = stmt
        .column_names()
        .into_iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>();
    println!("cols={:?}", col_names);
    let co = stmt.query_map(params![], |row| {
        println!("eee");
        let s = col_names
            .iter()
            .enumerate()
            .map(|(i, e)| format!("{}={}", e, format_blob(row.get_raw(i))))
            .collect::<Vec<String>>()
            .join(", ");
        println!("{}", s);
        Ok("z")
    })?;
    for entry in co {
        entry?;
    }*/

    Ok(())
}

fn format_blob(b: ValueRef) -> String {
    use ValueRef::*;
    match b {
        Null => "NULL".to_owned(),
        Integer(i) => format!("{}", i),
        Real(i) => format!("{}", i),
        Text(i) => format!("'{}'", String::from_utf8_lossy(i).replace("'", "''")),
        Blob(b) => format!("[blob {}B]", b.len()),
    }
}

/**
 *  adapted from https://github.com/jgallagher/rusqlite/blob/022266239233857faa7f0b415c1a3d5095d96a53/src/vtab/mod.rs#L629
 * sql injection safe? investigate
 * hello -> `hello`
 * he`lo -> `he``lo`
 *
 * we intentionally use the `e` syntax instead of "e" because of
 * "a misspelled double-quoted identifier will be interpreted as a string literal, rather than generating an error"
 * see https://www.sqlite.org/quirks.html#double_quoted_string_literals_are_accepted
 */
fn escape_sqlite_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace("`", "``"))
}

/**
 * this is needed because _parameters are not allowed in views_, so using prepared statements is not possible
 */
fn escape_sqlite_string(string: &str) -> String {
    format!("'{}'", string.replace("'", "''"))
}
