use std::fs::File;

use anyhow::Context;
use anyhow::Result;
use rusqlite::params;
use rusqlite::Connection;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use structopt::StructOpt;

#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
struct Title {
    tconst: String,
    titleType: String,
    primaryTitle: String,
    originalTitle: String,
    isAdult: i32,
    startYear: String,
    endYear: String,
    runtimeMinutes: String,
    genres: String,
}

fn pragmas(db: &Connection) -> Result<()> {
    //let want_page_size = 32768;
    //db.execute(&format!("pragma page_size = {};", want_page_size))
    //    .context("setup pragma 1")?;
    db.execute_batch(
        "
    pragma journal_mode = WAL;
    pragma foreign_keys = on;
    pragma temp_store = memory;
    pragma wal_autocheckpoint = 20;
    pragma synchronous = normal;
    pragma mmap_size = 30000000000;
    ",
    )?;
    let jm: String = db.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
    if &jm != "wal" {
        anyhow::bail!("journal mode is not wal");
    }
    Ok(())
}

#[derive(Debug, StructOpt)]
struct Config {
    #[structopt(short, long)]
    zstd_lib: String,
}

fn main() -> Result<()> {
    let config = Config::from_args();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    std::env::set_var("SQLITE_ZSTD_LOG", "debug");
    // before running, download https://datasets.imdbws.com/title.basics.tsv.gz
    // loads title_basics.tsv.gz, creates a json database and a database in normal form

    log::info!("loading csv");
    let data: Vec<Title> = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .quoting(false)
        .from_reader(File::open("benchmark/title.basics.tsv")?)
        .deserialize()
        .collect::<std::result::Result<Vec<Title>, csv::Error>>()
        .context("foo")?;
    {
        log::info!("creating columnar db");
        let mut columnar = Connection::open("benchmark/imdb-columnar.sqlite3")?;
        pragmas(&columnar)?;

        columnar.execute("create table title_basics(
            id integer primary key, tconst text, titleType text, primaryTitle text, originalTitle text, isAdult int, startYear text, endYear text, runtimeMinutes text, genres text)", params![])?;
        let db = columnar.transaction()?;
        let mut stmt = db.prepare("insert into title_basics values (?,?,?,?,?,?,?,?,?,?)")?;
        for ele in &data {
            stmt.execute(params![
                &Option::<String>::None,
                &ele.tconst,
                &ele.titleType,
                &ele.primaryTitle,
                &ele.originalTitle,
                &ele.isAdult,
                &ele.startYear,
                &ele.endYear,
                &ele.runtimeMinutes,
                &ele.genres
            ])?;
        }
        drop(stmt);
        db.commit()?;
    }
    {
        log::info!("creating json db");
        let mut jsondb = Connection::open("benchmark/imdb-json.sqlite3").unwrap();
        pragmas(&jsondb)?;
        jsondb.execute(
            "create table title_basics(
            id integer primary key, data text)",
            params![],
        )?;
        let tx = jsondb.transaction()?;
        let mut stmt = tx.prepare("insert into title_basics values (?, ?)")?;
        for ele in &data {
            stmt.execute(params![
                &Option::<String>::None,
                &serde_json::to_string(ele)?
            ])?;
        }
        drop(stmt);
        tx.commit()?;
        log::info!("vacuum-copying dbs");
        jsondb.execute(
            "vacuum into 'benchmark/imdb-json-nocompress.sqlite3'",
            params![],
        )?;
        jsondb.execute(
            "vacuum into 'benchmark/imdb-json-zstd-transparent.sqlite3'",
            params![],
        )?;
        jsondb.execute(
            "vacuum into 'benchmark/imdb-json-zstd-nodict.sqlite3'",
            params![],
        )?;
    }
    {
        log::info!("doing transparent compression");
        let db = Connection::open("benchmark/imdb-json-zstd-transparent.sqlite3").unwrap();
        pragmas(&db)?;
        db.load_extension(&config.zstd_lib, None)?;
        let config = json!({
            "table": "title_basics",
            "column": "data",
            "compression_level": 19,
            "dict_chooser": "'i' || (id/3000000)"
        });
        db.query_row(
            "select zstd_enable_transparent(?)",
            params![&serde_json::to_string(&config)?],
            |_| Ok(()),
        )?;
        db.query_row(
            "select zstd_incremental_maintenance(null, 1)",
            params![],
            |_| Ok(()),
        )?;
        db.execute("vacuum", params![])?;
    }
    {
        log::info!("doing nodict compression");
        let db = Connection::open("benchmark/imdb-json-zstd-nodict.sqlite3").unwrap();
        pragmas(&db)?;
        db.load_extension(&config.zstd_lib, None)?;
        let config = json!({
            "table": "title_basics",
            "column": "data",
            "compression_level": 19,
            "dict_chooser": "'[nodict]'"
        });
        db.query_row(
            "select zstd_enable_transparent(?)",
            params![&serde_json::to_string(&config)?],
            |_| Ok(()),
        )?;
        db.query_row(
            "select zstd_incremental_maintenance(null, 1)",
            params![],
            |_| Ok(()),
        )?;
        db.execute("vacuum", params![])?;
    }
    Ok(())
}
