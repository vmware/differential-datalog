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

package com.vmware.ddlog.ir;

import com.vmware.ddlog.translator.TranslationException;
import com.facebook.presto.sql.tree.Node;
import com.vmware.ddlog.util.ICastable;

import javax.annotation.Nullable;

public interface DDlogIRNode extends ICastable {
    default CodeFormatter format(CodeFormatter formatter) {
        formatter.append(this.toString());
        return formatter;
    }

    default <T> T checkNull(@Nullable T value) {
        if (value == null)
            this.error("Null pointer");
        assert value != null;
        return value;
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
