// https://www.sqlite.org/loadext.html
// https://github.com/jgallagher/rusqlite/issues/524#issuecomment-507787350

use rusqlite::Connection;
use rusqlite::ffi;
use std::os::raw::c_int;

#[expect(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_sqlitezstd_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut std::os::raw::c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    /* Insert here calls to
     **     sqlite3_create_function_v2(),
     **     sqlite3_create_collation_v2(),
     **     sqlite3_create_module_v2(), and/or
     **     sqlite3_vfs_register()
     ** to register the new features that your extension adds.
     */
    unsafe { Connection::extension_init2(db, pz_err_msg, p_api, init) }
}

fn init(db: Connection) -> rusqlite::Result<bool> {
    match crate::load(&db) {
        Ok(()) => {
            log::info!("[sqlite-zstd] initialized");
            Ok(false)
        }
        Err(e) => {
            log::error!("[sqlite-zstd] init error: {:?}", e);
            Err(rusqlite::Error::ModuleError(format!("{:?}", e)))
        }
    }
}
