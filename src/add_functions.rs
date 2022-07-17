use crate::dict_training::ZstdTrainDictAggregate;
use crate::util::*;
use crate::{basic::zstd_decompress_fn, transparent::*};

use crate::basic::zstd_compress_fn;
use rusqlite::functions::{Context, FunctionFlags};

pub fn add_functions(db: &rusqlite::Connection) -> anyhow::Result<()> {
    let nondeterministic = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY;
    let deterministic = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;

    let zstd_compress = |ctx: &Context| zstd_compress_fn(ctx, false).map_err(ah);
    let zstd_compress_col = |ctx: &Context| zstd_compress_fn(ctx, true).map_err(ah);

    let zstd_decompress = |ctx: &Context| zstd_decompress_fn(ctx, false).map_err(ah);
    let zstd_decompress_col = |ctx: &Context| zstd_decompress_fn(ctx, true).map_err(ah);
    //
    db.create_scalar_function("zstd_compress", 1, deterministic, zstd_compress)?;
    db.create_scalar_function("zstd_compress", 2, deterministic, zstd_compress)?;
    db.create_scalar_function("zstd_compress", 3, deterministic, zstd_compress)?;
    db.create_scalar_function("zstd_compress", 4, deterministic, zstd_compress)?;
    db.create_scalar_function("zstd_compress_col", 4, deterministic, zstd_compress_col)?;
    db.create_scalar_function("zstd_decompress", 2, deterministic, zstd_decompress)?;
    db.create_scalar_function("zstd_decompress", 3, deterministic, zstd_decompress)?;
    db.create_scalar_function("zstd_decompress", 4, deterministic, zstd_decompress)?;
    db.create_scalar_function("zstd_decompress_col", 4, deterministic, zstd_decompress_col)?;

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

    db.create_scalar_function("zstd_incremental_maintenance", 2, nondeterministic, |ctx| {
        zstd_incremental_maintenance(ctx).map_err(ah)
    })?;

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use anyhow::Context;
    use chrono::TimeZone;
    pub use pretty_assertions::{assert_eq, assert_ne};

    use rusqlite::{params, Connection};
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

    pub fn create_example_db(seed: Option<u64>, eles: i32) -> anyhow::Result<Connection> {
        let seed = seed.unwrap_or_else(|| thread_rng().gen());
        lazy_static::lazy_static! {
            // people use maybe 100 different apps
            static ref APP_NAMES: Vec<String> = names::Generator::with_naming(names::Name::Plain)
            .take(100)
            .collect();
            // of maybe 10 different categories
            static ref APP_TYPES: Vec<String> = names::Generator::with_naming(names::Name::Plain)
                .take(10)
                .collect();
        };
        let mut db = if std::env::var("TEST_TO_FILE").is_ok() {
            let db_fname = format!(
                "/tmp/foo.{}.sqlite3",
                rand::distributions::Uniform::from(0..10000).sample(&mut rand::thread_rng())
            );
            log::debug!("writing temp db to {}", db_fname);
            Connection::open(db_fname)?
        } else {
            Connection::open_in_memory().context("opening memory db")?
        };
        add_functions(&db).context("adding functions")?;
        db.execute_batch(
            "
            create table events (
                id integer primary key not null,
                timestamp text not null,
                data text not null,
                another_col text
            );
        ",
        )?;

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

        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let event_type_dist = WeightedIndex::new(&[10, 10, 1])?;
        let window_properties_dist = WeightedIndex::new(window_properties.iter().map(|e| e.0))?;
        let app_id_dist = rand::distributions::Uniform::from(0..100);
        let data = (0..eles).map(|_| match event_type_dist.sample(&mut rng) {
            0 => {
                let mut properties = BTreeMap::new();
                for _i in 1..rand::distributions::Uniform::from(100..1000).sample(&mut rng) {
                    let p = window_properties[window_properties_dist.sample(&mut rng)].1;
                    properties.insert(p.to_string(), "1".to_string());
                }
                EventData::OpenApplication {
                    id: app_id_dist.sample(&mut rng),
                    app_name: APP_NAMES.choose(&mut rng).unwrap().clone(),
                    app_type: APP_TYPES.choose(&mut rng).unwrap().clone(),
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
            {
                let mut insert = tx.prepare(
                    "insert into events (timestamp, data, another_col) values (?, ?, ?)",
                )?;
                let date = chrono::Utc.ymd(2021, 1, 1).and_hms(0, 0, 0);
                for (i, d) in data.enumerate() {
                    insert.execute(params![
                        (date + chrono::Duration::seconds(30) * (i as i32)).to_rfc3339(),
                        serde_json::to_string_pretty(&d)?,
                        "rustacean"
                    ])?;
                }
            }
            tx.commit()?;
        }
        Ok(db)
    }

    #[test]
    fn sanity() -> anyhow::Result<()> {
        let _db = create_example_db(None, 10).context("create eg db")?;
        Ok(())
    }

    fn test_strings() -> anyhow::Result<Vec<String>> {
        let data = vec![
            "hello this is a test",
            "foobar",
            "looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong",
            "nope",
        ];
        Ok(data.iter().map(|e| e.to_string()).collect())
    }

    #[test]
    fn compress_is_deterministic() -> anyhow::Result<()> {
        let db = create_example_db(None, 0)?;

        for eg in test_strings()? {
            let compressed1: Vec<u8> =
                db.query_row("select zstd_compress(?)", params![eg], |r| r.get(0))?;
            let compressed2: Vec<u8> =
                db.query_row("select zstd_compress(?)", params![eg], |r| r.get(0))?;
            assert_eq!(compressed1, compressed2)
        }

        Ok(())
    }

    #[test]
    fn compress_decompress_roundtrip() -> anyhow::Result<()> {
        let db = create_example_db(None, 0)?;

        for eg in test_strings()? {
            let compressed: Vec<u8> = db
                .query_row("select zstd_compress(?)", params![eg], |r| r.get(0))
                .context("compressing")?;
            let decompressed: String = db
                .query_row(
                    "select zstd_decompress(?, true)",
                    params![compressed],
                    |r| r.get(0),
                )
                .context("decompressing")?;
            assert_eq!(eg, decompressed)
        }

        Ok(())
    }

    #[test]
    fn decompress_type() -> anyhow::Result<()> {
        let db = create_example_db(None, 0)?;

        for eg in test_strings()? {
            let compressed: Vec<u8> =
                db.query_row("select zstd_compress(?)", params![eg], |r| r.get(0))?;
            let decompressed_text: String = db.query_row(
                "select zstd_decompress(?, true)",
                params![compressed],
                |r| r.get(0),
            )?;

            let decompressed_blob: Vec<u8> = db.query_row(
                "select zstd_decompress(?, false)",
                params![compressed],
                |r| r.get(0),
            )?;
            assert_eq!(decompressed_text.as_bytes(), decompressed_blob)
        }

        Ok(())
    }
    #[test]
    fn compress_with_dict_smaller() -> anyhow::Result<()> {
        let db = create_example_db(None, 100)?;

        let compressed1: Vec<u8> = db.query_row(
            "select zstd_compress((select data from events where id = 1), 5)",
            params![],
            |r| r.get(0),
        )?;

        let dict: Vec<u8> = db
            .query_row(
                "select zstd_train_dict(data, 1000, 100) from events",
                params![],
                |r| r.get(0),
            )
            .context("train dict")?;

        let compressed2: Vec<u8> = db
            .query_row(
                "select zstd_compress((select data from events where id = 1), 5, ?)",
                params![dict],
                |r| r.get(0),
            )
            .context("compress with dict")?;

        assert!(compressed1.len() > compressed2.len());

        let decompressed1: String = db
            .query_row("select zstd_decompress(?, 1)", params![compressed1], |r| {
                r.get(0)
            })
            .context("decompress 1")?;

        let decompressed2: String = db
            .query_row(
                "select zstd_decompress(?, 1, ?)",
                params![compressed2, dict],
                |r| r.get(0),
            )
            .context("decompress 2")?;

        assert_eq!(decompressed1, decompressed2);

        Ok(())
    }

    #[test]
    fn dict_saving_works() -> anyhow::Result<()> {
        let db = create_example_db(None, 100)?;

        let dict: i32 = db
            .query_row(
                "select zstd_train_dict_and_save(data, 1000, 100, null) from events",
                params![],
                |r| r.get(0),
            )
            .context("train dict")?;

        let uncompressed: String = db
            .query_row("select data from events where id = 1", params![], |r| {
                r.get(0)
            })
            .context("get data")?;

        let compressed2: Vec<u8> = db
            .query_row(
                "select zstd_compress((select data from events where id = 1), 5, ?)",
                params![dict],
                |r| r.get(0),
            )
            .context("compress with dict")?;

        let decompressed2: String = db
            .query_row(
                "select zstd_decompress(?, 1, ?)",
                params![compressed2, dict],
                |r| r.get(0),
            )
            .context("decompress 2")?;

        assert_eq!(uncompressed, decompressed2);

        Ok(())
    }

    #[test]
    fn levels() -> anyhow::Result<()> {
        let db = create_example_db(None, 5)?;
        /*db.prepare("select * from events")?
        .query_map(params![], |r| Ok(debug_row(r)))?
        .count();*/

        let mut st = db.prepare("select data from events")?;
        let eles: Vec<String> = st
            .query_map(params![], |r| r.get(0))
            .context("get sample")?
            .collect::<Result<_, _>>()?;

        for ele in eles {
            // let mut last_size = usize::MAX;
            for level in 1..24 {
                let compressed1: Vec<u8> = db
                    .query_row("select zstd_compress(?, ?)", params![ele, level], |r| {
                        r.get(0)
                    })
                    .context("compress")?;
                let decompressed1: String = db
                    .query_row(
                        "select zstd_decompress(?, ?)",
                        params![compressed1, 1],
                        |r| r.get(0),
                    )
                    .context("decompress")?;

                assert_eq!(ele, decompressed1);
                println!("l={}, size={}", level, compressed1.len());
                // assert!(compressed1.len() <= last_size);
                // last_size = compressed1.len();
            }
        }

        Ok(())
    }
}
