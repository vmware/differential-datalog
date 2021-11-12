/*
Copyright (c) 2021 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/* Logging Configuration API, (detailed documentation in `ddlog_log.h`) */

use once_cell::sync::Lazy;
use std::{collections, ffi, os::raw, sync};

type log_callback_t = Box<dyn Fn(i32, &str) + Send + Sync>;

struct LogConfig {
    default_callback: Option<log_callback_t>,
    default_level: i32,
    mod_callbacks: collections::HashMap<i32, (log_callback_t, i32)>,
}

impl LogConfig {
    fn new() -> LogConfig {
        LogConfig {
            default_callback: None,
            default_level: std::i32::MAX,
            mod_callbacks: collections::HashMap::new(),
        }
    }
}

/// Logger configuration for each module consists of the maximal enabled
/// log level (messages above this level are ignored) and callback.
static LOG_CONFIG: Lazy<sync::RwLock<LogConfig>> =
    Lazy::new(|| sync::RwLock::new(LogConfig::new()));

/// Logging API exposed to the DDlog program.
/// (see detailed documentation in `log.dl`)
#[allow(clippy::ptr_arg, clippy::trivially_copy_pass_by_ref)]
pub fn log(module: &i32, &level: &i32, msg: &String) {
    let cfg = LOG_CONFIG.read().unwrap();
    if let Some(&(ref cb, current_level)) = cfg.mod_callbacks.get(module) {
        if level <= current_level {
            cb(level, msg.as_str());
        }
    } else if level <= cfg.default_level && cfg.default_callback.is_some() {
        cfg.default_callback.as_ref().unwrap()(level, msg.as_str());
    }
}

/// `cb = None` - disables logging for the given module.
// NOTE: we set callback and log level simultaneously.  A more flexible API
// would allow changing log level without changing the callback.
pub fn log_set_callback(module: i32, cb: Option<log_callback_t>, max_level: i32) {
    let mut cfg = LOG_CONFIG.write().unwrap();
    match cb {
        Some(cb) => {
            cfg.mod_callbacks.insert(module, (cb, max_level));
        }
        None => {
            cfg.mod_callbacks.remove(&module);
        }
    }
}

/// Set default callback and log level for modules that were not configured
/// via `log_set_callback`.
pub fn log_set_default_callback(cb: Option<log_callback_t>, max_level: i32) {
    let mut cfg = LOG_CONFIG.write().unwrap();
    cfg.default_callback = cb;
    cfg.default_level = max_level;
}

/// C bindings for the config API
#[no_mangle]
#[cfg(feature = "c_api")]
pub unsafe extern "C" fn ddlog_log_set_callback(
    module: raw::c_int,
    cb: Option<extern "C" fn(arg: libc::uintptr_t, level: raw::c_int, msg: *const raw::c_char)>,
    cb_arg: libc::uintptr_t,
    max_level: raw::c_int,
) {
    let _ = std::panic::catch_unwind(|| match cb {
        Some(cb) => log_set_callback(
            module as i32,
            Some(Box::new(move |level, msg| {
                cb(
                    cb_arg,
                    level as raw::c_int,
                    ffi::CString::new(msg).unwrap_or_default().as_ptr(),
                )
            })),
            max_level as i32,
        ),
        None => log_set_callback(module as i32, None, max_level as i32),
    });
}

#[no_mangle]
#[cfg(feature = "c_api")]
pub unsafe extern "C" fn ddlog_log_set_default_callback(
    cb: Option<extern "C" fn(arg: libc::uintptr_t, level: raw::c_int, msg: *const raw::c_char)>,
    cb_arg: libc::uintptr_t,
    max_level: raw::c_int,
) {
    let _ = std::panic::catch_unwind(|| match cb {
        Some(cb) => log_set_default_callback(
            Some(Box::new(move |level, msg| {
                cb(
                    cb_arg,
                    level as raw::c_int,
                    ffi::CString::new(msg).unwrap_or_default().as_ptr(),
                )
            })),
            max_level as i32,
        ),
        None => log_set_default_callback(None, max_level as i32),
    });
}
