#![warn(clippy::print_stdout)]

use rusqlite::Connection;
use util::init_logging;

#[cfg(feature = "build_extension")]
mod create_extension;

mod add_functions;
mod basic;
mod dict_management;
mod dict_training;
mod transparent;
mod util;

pub use log::LevelFilter as LogLevel;

/// Loads the sqlite extension with the default log level (INFO)
pub fn load(connection: &Connection) -> anyhow::Result<()> {
    load_with_loglevel(connection, LogLevel::Info)
}

/// Loads the sqlite extension with the given log level
pub fn load_with_loglevel(
    connection: &Connection,
    default_log_level: LogLevel,
) -> anyhow::Result<()> {
    init_logging(default_log_level);
    crate::add_functions::add_functions(connection)
}
