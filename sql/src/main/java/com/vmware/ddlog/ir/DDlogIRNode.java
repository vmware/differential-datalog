/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice (including the next paragraph) shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.vmware.ddlog.ir;

import com.vmware.ddlog.translator.TranslationException;
import com.facebook.presto.sql.tree.Node;

import javax.annotation.Nullable;

public interface DDlogIRNode {
    String toString();

    default CodeFormatter format(CodeFormatter formatter) {
        formatter.append(this.toString());
        return formatter;
    }

    default <T> T checkNull(@Nullable T value) {
        if (value == null)
            this.error("Null pointer");
        return value;
    }

    @Nullable
    default <T> T as(Class<T> clazz) {
        try {
            return clazz.cast(this);
        } catch (ClassCastException e) {
            return null;
        }
    }

    default <T> T as(Class<T> clazz, @Nullable String failureMessage) {
        T result = this.as(clazz);
        if (result == null) {
            if (failureMessage == null)
                failureMessage = this.getClass().getName() + " is not an instance of " + clazz.toString();
            this.error(failureMessage);
        }
        assert result != null;
        return result;
    }

    default <T> T to(Class<T> clazz) {
        return this.as(clazz, null);
    }

    default void error(String message) {
        throw new TranslationException(message, this.getNode());
    }

    default <T> boolean is(Class<T> clazz) {
        return this.as(clazz) != null;
    }

    /**
     * This is the SQL IR node that was compiled to produce
     * this DDlogIR node.
     */
    @Nullable
    public Node getNode();
}
