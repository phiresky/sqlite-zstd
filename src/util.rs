





use rusqlite::types::{ValueRef};



/*fn context_to_db(ctx: &Context) -> Connection {
    let handle = rusqlite::ffi::sqlite3_context_db_handle(ctx.ctx);
}*/

pub fn debug_row(r: &rusqlite::Row) {
    let cols = r.column_names();
    for (i, name) in cols.iter().enumerate() {
        print!("{}={} ", name, format_blob(r.get_raw(i)))
    }
    println!();
}

/// format an expression while escaping given values as sqlite identifiers
/// needed since prepared query parameters can't be used in identifier position
/// (i'm too dumb for recursive macros)
#[macro_export]
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

pub fn ah(e: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(format!("{:?}", e).into())
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
    format!("`{}`", identifier.replace("`", "``"))
}

/**
 * this is needed sometimes because _parameters are not allowed in views_, so using prepared statements is not possible :/
 */
pub fn escape_sqlite_string(string: &str) -> String {
    format!("'{}'", string.replace("'", "''"))
}
