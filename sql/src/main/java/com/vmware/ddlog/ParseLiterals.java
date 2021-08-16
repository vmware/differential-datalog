/*
 * Copyright (c) 2021 VMware, Inc.
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
 *
 */

package com.vmware.ddlog;

import ddlogapi.DDlogException;
import ddlogapi.DDlogRecord;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.util.SqlBasicVisitor;

/*
 * Translates literals into corresponding DDlogRecord instances
 */
class ParseLiterals extends SqlBasicVisitor<DDlogRecord> {
    @Override
    public DDlogRecord visit(final SqlLiteral sqlLiteral) {
        switch (sqlLiteral.getTypeName()) {
            case BOOLEAN:
                return new DDlogRecord(sqlLiteral.booleanValue());
            case DECIMAL:
                return new DDlogRecord(sqlLiteral.intValue(false));
            case CHAR:
                try {
                    return new DDlogRecord(sqlLiteral.toValue());
                } catch (final DDlogException ignored) {
                }
            case NULL:
                return null;
            default:
                throw new UnsupportedOperationException(sqlLiteral.toValue());
        }
    }
}
