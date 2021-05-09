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

/*
 * C prototypes for the logging API (see `log.dl`, `log.rs`)
 */

/*
 * Set logging callback and log level for a given module.
 *
 * `module`    - Opaque module identifier.  Can represent module, subsystem, log
 *               stream, etc.
 * `cb`        - Callback to be invoked for each log message with the given module
 *               id and log level `<= max_level`.  Passing `NULL` disables
 *               logging for the given module.
 * `max_level` - Don't invoke the callback for messages whose log level is
 *               `> max_level`.
 */
extern void ddlog_log_set_callback(
        int module,
        void (*cb)(uintptr_t arg, int level, const char* msg),
        uintptr_t cb_arg,
        int max_level);

/*
 * Set default callback and log level for modules that were not configured
 * via `ddlog_log_set_callback`.
 *
 * `cb`        - Callback to be invoked for each log message with level `<= max_level`.
 *               Passing `NULL` disables logging by default.
 * `max_level` - Don't invoke the callback for messages whose log level is
 *               `> max_level`.
 */
extern void ddlog_log_set_default_callback(
        void (*cb)(uintptr_t arg, int level, const char* msg),
        uintptr_t cb_arg,
        int max_level);
