#![cfg(feature = "benchmark")]

use anyhow::Context;
use anyhow::Result;
use rand::seq::SliceRandom;
use rusqlite::{params, Connection, OpenFlags};
use std::path::{Path, PathBuf};
use std::{io::Write, time::Instant};
use structopt::StructOpt;
#[derive(Debug, StructOpt)]
struct Config {
    #[structopt(short, long)]
    input_db: Vec<String>,
    #[structopt(short, long)]
    location: Vec<String>,
    #[structopt(short, long)]
    zstd_lib: String,
    #[structopt(short, long)]
    hot_cache: bool,
    #[structopt(short, long)]
    iterations: i32,
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
trait Bench {
    fn name(&self) -> &str;
    fn execute(&self, conn: &Connection) -> Result<i64>;
}

type DbId = i64;
struct SelectBench {
    name: &'static str,
    ids: Vec<DbId>,
}

impl SelectBench {
    fn prepare_sequential(conn: &Connection) -> Result<Box<dyn Bench>> {
        Ok(Box::new(SelectBench {
        name: "Select 1000 sequential (compressed) values",
            ids: conn.prepare("select id from title_basics where id >= (select id from title_basics order by random() limit 1) order by id asc limit 1000")?.query_map(params![], |r| r.get(0))?.collect::<Result<_, _>>()?
        }))
    }
    fn prepare_random(conn: &Connection) -> Result<Box<dyn Bench>> {
        Ok(Box::new(SelectBench {
            name: "Select 1000 random (compressed) values",
            ids: conn
                .prepare("select id from title_basics order by random() limit 1000")?
                .query_map(params![], |r| r.get(0))?
                .collect::<Result<_, _>>()?,
        }))
    }
}

impl Bench for SelectBench {
    fn name(&self) -> &str {
        self.name
    }
    fn execute(&self, conn: &Connection) -> Result<i64> {
        let mut stmt = conn.prepare("select data from title_basics where id = ?")?;
        let mut total_len = 0;
        for id in &self.ids {
            let data: String = stmt.query_row(params![id], |r| r.get(0))?;
            total_len += data.len();
        }

        // eprintln!("total bytes got: {}", total_len);
        Ok(self.ids.len() as i64)
    }
}

struct UpdateBench {
    name: &'static str,
    values: Vec<(DbId, String)>,
}
impl UpdateBench {
    fn prepare_random(conn: &Connection) -> Result<Box<dyn Bench>> {
        let ids: Vec<DbId> = conn
            .prepare("select id from title_basics order by random() limit 1000")?
            .query_map(params![], |r| r.get(0))?
            .collect::<Result<_, _>>()?;

        let values: Vec<String> = conn
            .prepare("select data from title_basics order by random() limit 1000")?
            .query_map(params![], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        Ok(Box::new(UpdateBench {
            name: "Update 1000 random (compressed) values",
            values: ids.into_iter().zip(values).collect(),
        }))
    }
    fn prepare_sequential(conn: &Connection) -> Result<Box<dyn Bench>> {
        let ids: Vec<DbId> = conn
            .prepare("select id from title_basics where id >= (select id from title_basics order by random() limit 1) order by id asc limit 1000")?
            .query_map(params![], |r| r.get(0))?
            .collect::<Result<_, _>>()?;

        let values: Vec<String> = conn
            .prepare("select data from title_basics order by random() limit ?")?
            .query_map(params![ids.len()], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        Ok(Box::new(UpdateBench {
            name: "Update 1000 sequential (compressed) values",
            values: ids.into_iter().zip(values).collect(),
        }))
    }
}
impl Bench for UpdateBench {
    fn name(&self) -> &str {
        self.name
    }
    fn execute(&self, conn: &Connection) -> Result<i64> {
        conn.execute("begin", params![])?;
        let mut stmt = conn.prepare("update title_basics set data = ? where id = ?")?;
        for (id, value) in &self.values {
            stmt.execute(params![value, id])?;
        }
        conn.execute("commit", params![])?;
        Ok(self.values.len() as i64)
    }
}
struct InsertBench {
    name: &'static str,
    values: Vec<String>,
}
impl InsertBench {
    fn prepare_random(conn: &Connection) -> Result<Box<dyn Bench>> {
        let values: Vec<String> = conn
            .prepare("select data from title_basics order by random() limit 1000")?
            .query_map(params![], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        Ok(Box::new(InsertBench {
            name: "Insert 1000 new values",
            values,
        }))
    }
}
impl Bench for InsertBench {
    fn name(&self) -> &str {
        self.name
    }
    fn execute(&self, conn: &Connection) -> Result<i64> {
        conn.execute("begin", params![])?;
        let mut stmt = conn.prepare("insert into title_basics (data) values (?)")?;
        for value in &self.values {
            stmt.execute(params![value])?;
        }
        conn.execute("commit", params![])?;
        Ok(self.values.len() as i64)
    }
}

fn drop_caches() -> Result<()> {
    eprintln!("dropping caches");
    assert!(std::process::Command::new("sync").status()?.success());
    std::fs::OpenOptions::new()
        .read(false)
        .write(true)
        .open("/proc/sys/vm/drop_caches")
        .context("Could not open drop caches")?
        .write_all(b"3")
        .context("Could not drop caches")?;
    Ok(())
}

struct BenchTarget {
    total_count: i64,
    total_duration_s: f64,
    path: PathBuf,
}
fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        panic!("benching must be done in prod mode, otherwise the results are useless");
    }
    let config = Config::from_args();
    //let input_db = Connection::open_with_flags(config.input_db)?;

    let its_per_bench = config.iterations;

    println!("location,db filename,test name,iterations/s,number of samples");

    let benches: Vec<Vec<_>> = {
        let db1 =
            Connection::open_with_flags(&config.input_db[0], OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        type Preparer = Box<dyn Fn(&Connection) -> Result<Box<dyn Bench>>>;
        let preparers: Vec<Preparer> = vec![
            Box::new(SelectBench::prepare_random),
            Box::new(SelectBench::prepare_sequential),
            Box::new(UpdateBench::prepare_random),
            Box::new(UpdateBench::prepare_sequential),
            Box::new(InsertBench::prepare_random),
        ];
        preparers
            .iter()
            .map(|preparer| {
                eprintln!("running preparer {its_per_bench} times");
                (0..its_per_bench)
                    .map(|i| preparer(&db1))
                    .collect::<Result<_, _>>()
                    .context("preparing benches")
            })
            .collect::<Result<_, _>>()?
    };

    for locjoi in config.location {
        let (location_name, location) = {
            let vec: Vec<_> = locjoi.splitn(2, ':').collect();
            (vec[0], vec[1])
        };
        eprintln!("{} at {}", location_name, location);

        let db_paths = config
            .input_db
            .iter()
            .map(|input_db| {
                let pb = PathBuf::from(input_db);
                let file_name = pb.file_name().unwrap();

                let db_path = Path::new(&location).join(file_name);
                if !db_path.exists() {
                    eprintln!("copying {} -> {}", input_db, db_path.to_string_lossy());
                    std::fs::copy(&input_db, &db_path)?;
                } else {
                    eprintln!(
                        "{} already exists, assuming it's the same",
                        file_name.to_string_lossy()
                    );
                }
                Ok(db_path)
            })
            .collect::<Result<Vec<_>>>()?;
        for bench_its in &benches {
            // eprintln!("{locjoi} benchmark {}", bench_its[0].name());
            let mut targets: Vec<_> = db_paths
                .iter()
                .map(|path| BenchTarget {
                    total_count: 0,
                    total_duration_s: 0.0,
                    path: path.clone(),
                })
                .collect();
            for (i, bench) in bench_its.iter().enumerate() {
                eprintln!(
                    "{locjoi} benchmark {} iteration {i} / {its_per_bench}",
                    bench.name()
                );
                if !config.hot_cache {
                    drop_caches()?;
                }
                // shuffle to make sure there is no crosstalk
                targets.shuffle(&mut rand::thread_rng());

                for target in targets.iter_mut() {
                    let db = Connection::open(&target.path)?;
                    pragmas(&db).context("Could not set pragmas")?;
                    db.load_extension(&config.zstd_lib, None)?;
                    let before = Instant::now();
                    target.total_count += bench.execute(&db).context("executing bench")?;
                    target.total_duration_s += before.elapsed().as_secs_f64();
                }
            }
            targets.sort_by_key(|e| e.path.clone());
            for target in &targets {
                println!(
                    "{},{},{},{:.0},{}",
                    location_name,
                    target.path.file_name().unwrap().to_string_lossy(),
                    bench_its[0].name(),
                    target.total_count as f64 / target.total_duration_s,
                    target.total_count
                );
            }
        }
    }

    Ok(())
}
