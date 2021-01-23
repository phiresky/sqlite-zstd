#![cfg(feature = "benchmark")]

use anyhow::Context;
use rusqlite::{params, Connection, OpenFlags};
use std::{fs::File, path::Path};
use std::{io::Write, time::Instant};
use structopt::StructOpt;
#[derive(Debug, StructOpt)]
struct Config {
    #[structopt(short, long)]
    input_db_1: String,
    #[structopt(short, long)]
    input_db_2: String,
    #[structopt(short, long)]
    hdd_location: String,
    #[structopt(short, long)]
    sdd_location: String,
    #[structopt(short, long)]
    zstd_lib: String,
}

fn pragmas(db: &Connection) -> anyhow::Result<()> {
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
trait Bench {
    fn execute(&self, conn: &Connection) -> anyhow::Result<i64>;
}
struct SeqSelectBench {
    ids: Vec<String>,
}

impl SeqSelectBench {
    fn prepare(conn: &Connection) -> anyhow::Result<SeqSelectBench> {
        Ok(SeqSelectBench {
            ids: conn.prepare("select id from events where timestamp_unix_ms >= (select timestamp_unix_ms from events order by random() limit 1) order by timestamp_unix_ms asc limit 10000")?.query_map(params![], |r| r.get(0))?.collect::<Result<_, _>>()?
        })
    }
}
impl Bench for SeqSelectBench {
    fn execute(&self, conn: &Connection) -> anyhow::Result<i64> {
        let mut stmt = conn.prepare("select data from events where id = ?")?;
        let mut total_len = 0;
        for id in &self.ids {
            let data: String = stmt.query_row(params![id], |r| r.get(0))?;
            total_len += data.len();
        }

        println!("total bytes got: {}", total_len);
        Ok(self.ids.len() as i64)
    }
}
fn main() -> anyhow::Result<()> {
    if cfg!(debug_assertions) {
        panic!("benching must be done in prod mode, otherwise the results are useless");
    }
    let config = Config::from_args();
    //let input_db = Connection::open_with_flags(config.input_db)?;

    let table = "events";
    let time_column = "timestamp_unix_ms";

    for (name, location) in vec![("hdd", config.hdd_location)] {
        println!("{} at {}", name, location);

        let db_path_1 = Path::new(&location).join("db1.sqlite3");
        let db_path_2 = Path::new(&location).join("db2.sqlite3");
        std::fs::copy(&config.input_db_1, &db_path_1)?;
        std::fs::copy(&config.input_db_2, &db_path_2)?;

        println!("dropping caches");
        assert!(std::process::Command::new("sync").status()?.success(), true);
        std::fs::OpenOptions::new()
            .read(false)
            .write(true)
            .open("/proc/sys/vm/drop_caches")
            .context("Could not open drop caches")?
            .write_all(b"3")
            .context("Could not drop caches")?;

        let mut db1 = Connection::open(db_path_1)?;
        pragmas(&db1).context("Could not set pragmas")?;
        db1.load_extension(&config.zstd_lib, None)?;
        let mut db2 = Connection::open(db_path_2)?;
        pragmas(&db2).context("Could not set pragmas")?;
        db2.load_extension(&config.zstd_lib, None)?;

        /*let (first_date, last_date): (i64, i64) = db.query_row(
            &format!(
                "select min({t}), max({t}) from {tbl}",
                t = time_column,
                tbl = table
            ),
            params![],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        println!("min time {}, max time {}", first_date, last_date);*/

        let benches: Vec<_> = (0..100)
            .map(|i| SeqSelectBench::prepare(&db1))
            .collect::<Result<_, _>>()
            .context("preparing benches")?;

        for db in &[&db1, &db2] {
            let mut total_count: i64 = 0;
            let before = Instant::now();
            for bench in &benches {
                total_count += bench.execute(db).context("executing bench")?;
            }
            let duration_s = before.elapsed().as_secs_f64();
            println!(
                "{} iterations/s (n={})",
                total_count as f64 / duration_s,
                total_count
            );
        }
    }

    Ok(())
}
