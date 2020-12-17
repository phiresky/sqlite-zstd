#![warn(clippy::print_stdout)]
#![warn(clippy::print_stdout)]

use rusqlite::Connection;

#[cfg(feature = "build_extension")]
mod create_extension;

mod dict_management;
mod dict_training;
mod transparent;
mod util;
mod zstd_fns;

pub fn load_functions(connection: &Connection) -> anyhow::Result<()> {
    crate::zstd_fns::add_functions(&connection)
}
