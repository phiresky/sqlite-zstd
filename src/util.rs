use log::LevelFilter;

pub fn ensure_dicts_table_exists(db: &rusqlite::Connection) -> rusqlite::Result<()> {
    db.execute_batch(
        "
        create table if not exists _zstd_dicts (
            id integer primary key autoincrement,
            chooser_key text unique,
            dict blob not null
        );
        insert or ignore into _zstd_dicts values (-1, '[nodict]', ''); -- only added so foreign key is fulfilled
        ",
    )?;
    Ok(())
}

/// format an expression while escaping given values as sqlite identifiers
/// needed since prepared query parameters can't be used in identifier position
#[doc(hidden)]
#[macro_export]
macro_rules! format_sqlite {
    ($x:expr, $($y:expr),*) => {
        format!($x, $(escape_sqlite_identifier($y),)*)
    };
}

pub fn ah(e: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(format!("{:?}", e).into())
}

/*pub fn debug_row(r: &rusqlite::Row) {
    let cols = r.column_names();
    for (i, name) in cols.iter().enumerate() {
        print!("{}={} ", name, format_blob(r.get_ref_unwrap(i)))
    }
    println!();
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
}*/

///
/// adapted from https://github.com/jgallagher/rusqlite/blob/022266239233857faa7f0b415c1a3d5095d96a53/src/vtab/mod.rs#L629
/// sql injection safe? investigate
/// hello -> `hello`
/// he`lo -> `he``lo`
///
/// we intentionally use the `e` syntax instead of "e" because of
/// "a misspelled double-quoted identifier will be interpreted as a string literal, rather than generating an error"
/// see https://www.sqlite.org/quirks.html#double_quoted_string_literals_are_accepted
///
pub fn escape_sqlite_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

/**
 * this is needed sometimes because _parameters are not allowed in views_, so using prepared statements is not possible :/
 */
/*pub fn escape_sqlite_string(string: &str) -> String {
    format!("'{}'", string.replace("'", "''"))
}*/

pub fn init_logging(default_level: LevelFilter) {
    if std::env::var("SQLITE_ZSTD_LOG").is_err() {
        std::env::set_var("SQLITE_ZSTD_LOG", format!("{}", default_level));
    }
    env_logger::try_init_from_env(env_logger::Env::new().filter("SQLITE_ZSTD_LOG")).ok();
}
