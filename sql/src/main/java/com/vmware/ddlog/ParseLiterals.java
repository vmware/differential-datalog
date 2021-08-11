/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
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
