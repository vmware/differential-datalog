/*
 * Copyright (c) 2018-2021 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.vmware.ddlog.util.sql;

/**
 * Base abstract class for translators from any SQL dialect to the Presto dialect, which is used in Translator.
 */
public abstract class ToPrestoTranslator {
    /**
     * Translates given SQL statement to Presto dialect.
     * @param sql
     * @return
     */
    public abstract String toPresto(String sql);

    /**
     * Return what is equivalent to a PrestoToPrestoTranslator, which simply returns the SQL statement.
     * This method can be used to fetch a translator used when the user already passes SQL in the Presto dialect.
     * @return
     */
    public static ToPrestoTranslator noopTranslator() {
        return new ToPrestoTranslator() {
            @Override
            public String toPresto(String sql) {
                return sql;
            }
        };
    }
}