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

package com.vmware.ddlog.translator.environment;

import com.vmware.ddlog.ir.DDlogExpression;
import com.vmware.ddlog.util.IndentStringBuilder;
import com.vmware.ddlog.util.Printable;

import javax.annotation.Nullable;

/**
 * An environment converts identifiers into expressions.
 */
public abstract class IEnvironment implements Printable {
    /**
     * Make a copy of the data in this environment.
     */
    public abstract IEnvironment cloneEnv();

    /**
     * Lookup an identifier in this environment.
     * @param identifier  Identifier to lookup.
     * @return  null if the identifier is not found.
     * Identifiers are usually column names.
     */
    @Nullable
    public abstract DDlogExpression lookupIdentifier(String identifier);

    /**
     * Lookup a relation name in this environment.
     * @param identifier  Identifier to lookup.
     * @return  null if the identifier is not found.
     */
    @Nullable
    public abstract IEnvironment lookupRelation(String identifier);

    /**
     * Build an identifier where things are looked up first in this and then in other.
     * @param other  Environment to look up things that are not found in this one.
     */
    public IEnvironment stack(IEnvironment other) {
        return new StackedEnvironment(this, other);
    }

    @Override
    public String toString() {
        IndentStringBuilder builder = new IndentStringBuilder();
        return this.toString(builder).toString();
    }

    public abstract IndentStringBuilder toString(IndentStringBuilder builder);
}
