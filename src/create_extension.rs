// https://www.sqlite.org/loadext.html
// https://github.com/jgallagher/rusqlite/issues/524#issuecomment-507787350

use rusqlite::ffi;
use std::os::raw::c_int;

#[no_mangle]
pub extern "C" fn sqlite3_sqlitezstd_init(
    db: *mut ffi::sqlite3,
    _pz_err_msg: &mut &mut std::os::raw::c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    // SQLITE_EXTENSION_INIT2 equivalent
    unsafe {
        ffi::sqlite3_api = p_api;
    }
    /* Insert here calls to
     **     sqlite3_create_function_v2(),
     **     sqlite3_create_collation_v2(),
     **     sqlite3_create_module_v2(), and/or
     **     sqlite3_vfs_register()
     ** to register the new features that your extension adds.
     */
    match init(db) {
        Ok(()) => {
            log::info!("[sqlite-zstd] initialized");
            ffi::SQLITE_OK
        }
        Err(e) => {
            log::error!("[sqlite-zstd] init error: {:?}", e);
            ffi::SQLITE_ERROR
        }
    }
}

fn init(db_handle: *mut ffi::sqlite3) -> anyhow::Result<()> {
    let db = unsafe { rusqlite::Connection::from_handle(db_handle)? };

    crate::load(&db)?;
    Ok(())
}
