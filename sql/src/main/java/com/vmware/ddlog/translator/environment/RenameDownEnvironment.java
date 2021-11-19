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

import javax.annotation.Nullable;
import java.util.HashMap;

/**
 * This class renames identifiers before looking them up in children environments.
 */
public class RenameDownEnvironment extends IEnvironment {
    final IEnvironment base;
    /**
     * Map describing how identifiers are renamed before looking them up.
     */
    final HashMap<String, String> rename;

    public RenameDownEnvironment(IEnvironment base, HashMap<String, String> rename) {
        this.base = base;
        this.rename = rename;
    }

    @Override
    public IEnvironment cloneEnv() {
        return new RenameDownEnvironment(this.base.cloneEnv(), this.rename);
    }

    @Nullable
    @Override
    public DDlogExpression lookupIdentifier(String identifier) {
        String lookup = identifier;
        if (this.rename.containsKey(identifier))
            lookup = this.rename.get(identifier);
        return this.base.lookupIdentifier(lookup);
    }

    @Nullable
    @Override
    public IEnvironment lookupRelation(String identifier) {
        String lookup = identifier;
        if (this.rename.containsKey(identifier))
            lookup = this.rename.get(identifier);
        IEnvironment res = this.base.lookupRelation(lookup);
        if (res == null)
            return res;
        return new RenameDownEnvironment(res, this.rename);
    }

    @Override
    public IndentStringBuilder toString(IndentStringBuilder builder) {
        return builder.append("RenameDown")
                .append(this.rename.toString())
                .increase()
                .append(this.base)
                .decrease();
    }
}
