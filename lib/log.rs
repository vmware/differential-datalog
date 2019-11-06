use std::collections;
use std::ffi;
use std::sync;

type log_callback_t = Box<dyn Fn(log_log_level_t, &str) + Send + Sync>;

lazy_static! {
    /* Logger configuration for each module consists of the maximal enabled
     * log level (messages above this level are ignored) and callback.
     */
    static ref LOG_CONFIG: sync::RwLock<collections::HashMap<log_module_t, (log_callback_t, log_log_level_t)>> = {
        sync::RwLock::new(collections::HashMap::new())
    };
}

/*
 * Logging API exposed to the DDlog program.
 * (see detailed documentation in `log.dl`)
 */
pub fn log_log(module: &log_module_t, level: &log_log_level_t, msg: &String) -> bool {
    if let Some((cb, current_level)) = LOG_CONFIG.read().unwrap().get(&module) {
        if *level <= *current_level {
            cb(*level, msg.as_str());
        }
    };
    true
}

/*
 * Configuration API
 * (detailed documentation in `ddlog_log.h`)
 *
 * `cb = None` - disables logging for the given module.
 *
 * NOTE: we set callback and log level simultaneously.  A more flexible API
 * would allow changing log level without changing the callback.
 */
pub fn log_set_callback(
    module: log_module_t,
    cb: Option<log_callback_t>,
    max_level: log_log_level_t,
) {
    match cb {
        Some(cb) => {
            LOG_CONFIG.write().unwrap().insert(module, (cb, max_level));
        }
        None => {
            LOG_CONFIG.write().unwrap().remove(&module);
        }
    }
}

/*
 * C bindings for the config API
 */
#[no_mangle]
pub unsafe extern "C" fn ddlog_log_set_callback(
    module: raw::c_int,
    cb: Option<extern "C" fn(arg: libc::uintptr_t, level: raw::c_int, msg: *const raw::c_char)>,
    cb_arg: libc::uintptr_t,
    max_level: raw::c_int,
) {
    match cb {
        Some(cb) => log_set_callback(
            module as log_module_t,
            Some(Box::new(move |level, msg| {
                cb(
                    cb_arg,
                    level as raw::c_int,
                    ffi::CString::new(msg).unwrap_or_default().as_ptr(),
                )
            })),
            max_level as log_log_level_t,
        ),
        None => log_set_callback(module as log_module_t, None, max_level as log_log_level_t),
    }
}
