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

package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.*;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Represents a DDlog relation name.
 * Relation names have some different rules from SQL table names, so they cannot be identical.
 */
public class RelationName {
    @Nullable
    private final String originalName;
    public final String name;
    @Nullable
    public final Node node;

    public RelationName(String name, @Nullable String originalName, @Nullable Node node) {
        this.name = name;
        this.node = node;
        this.originalName = originalName;
        if (!isValid(name))
            throw new RuntimeException("Invalid relation name " + name);
    }

    public static boolean isValid(String name) {
        if (name.length() == 0)
            return false;
        char first = name.charAt(0);
        return Character.isLetter(first) && !Character.isLowerCase(first);
    }

    public static String makeRelationName(String tableName) {
        return "R" + tableName.toLowerCase();
    }

    public String toString() {
        return this.name;
    }

    public String originalName() {
        return Objects.requireNonNull(this.originalName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationName that = (RelationName) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }
}
