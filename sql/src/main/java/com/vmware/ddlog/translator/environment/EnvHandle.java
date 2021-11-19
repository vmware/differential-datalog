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

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Provides the basic environment operations.
 */
public class EnvHandle {
    IEnvironment environment;
    @Nullable
    IEnvironment last = null;

    public EnvHandle() {
        this.environment = new EmptyEnvironment();
    }

    /**
     * Cleaup the scope stack.
     */
    public void exitAllScopes() {
        this.environment = new EmptyEnvironment();
    }

    /**
     * Pop the last scope from the stack.
     */
    public void exitLastScope() {
        if (this.environment instanceof StackedEnvironment) {
            this.environment = ((StackedEnvironment)this.environment).bottom;
            return;
        }
        throw new RuntimeException("Not a stack");
    }

    public void save() {
        if (this.last != null)
            throw new RuntimeException("Environment already saved");
        this.last = this.environment;
        this.environment = new EmptyEnvironment();
    }

    public void restore() {
        if (this.last == null)
            throw new RuntimeException("No environment saved");
        this.environment = this.last;
        this.last = null;
    }

    @Nullable
    public DDlogExpression lookupIdentifier(String identifier) {
        return this.environment.lookupIdentifier(identifier);
    }

    @Nullable
    public IEnvironment lookupRelation(String identifier) {
        return this.environment.lookupRelation(identifier);
    }

    public void stack(IEnvironment environment) {
        this.environment = new StackedEnvironment(environment, this.environment);
    }

    public void stack(EnvHandle container) {
        this.stack(container.environment);
    }

    public void renameDown(String from, String to) {
        HashMap<String, String> map = new HashMap<>();
        map.put(from, to);
        this.environment = new RenameDownEnvironment(this.environment, map);
    }

    public void renameDown(HashMap<String, String> map) {
        this.environment = new RenameDownEnvironment(this.environment, map);
    }

    public void delete(String delete) {
        HashSet<String> del = new HashSet<>();
        del.add(delete);
        this.environment = new DeleteEnvironment(this.environment, del);
    }

    public void renameUp(String originalName, String newName, HashMap<String, String> columnRename) {
        this.environment = new RenameUpEnvironment(this.environment, originalName, newName, columnRename);
    }

    @Override
    public String toString() {
        return this.environment.toString();
    }

    public void set(IEnvironment environment) {
        this.environment = environment;
    }
}
