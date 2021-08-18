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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import java.io.StringReader;

/**
 * Some utility methods regarding the Apache Calcite tooling environment.
 */
public class CalciteUtils {

    /**
     * Initialize and return a Calcite DDL Parser.
     * @param sql  Statement to initialize parser with.
     */
    public static SqlAbstractParserImpl createCalciteParser(CalciteSqlStatement sql) {
        SqlAbstractParserImpl ret = SqlDdlParserImpl.FACTORY.getParser(new StringReader(sql.getStatement()));

        /*
         * Currently, SqlDdlParserImpl doesn't have a constructor that accepts config parameters,
         * so we have to set them manually.
         */
        ret.setUnquotedCasing(Casing.TO_LOWER);
        ret.setQuotedCasing(Casing.TO_LOWER);
        ret.setIdentifierMaxLength(100);

        return ret;
    }
}
