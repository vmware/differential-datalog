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

import com.vmware.ddlog.util.Linq;

import java.util.ArrayList;
import java.util.List;

public class DDlogProgram implements DDlogIRNode {
    // We are missing some of the fields that can never be generated from SQL
    public final List<DDlogTypeDef> typedefs;
    public final List<DDlogFunction> functions;
    public final List<DDlogRelation> relations;
    public final List<DDlogRule> rules;
    public final List<DDlogImport> imports;

    public DDlogProgram(List<DDlogTypeDef> typedefs, List<DDlogFunction> functions, List<DDlogRelation> relations, List<DDlogRule> rules, List<DDlogImport> imports) {
        this.typedefs = typedefs;
        this.functions = functions;
        this.relations = relations;
        this.rules = rules;
        this.imports = imports;
    }

    public DDlogProgram() {
        this(new ArrayList<DDlogTypeDef>(), new ArrayList<DDlogFunction>(),
                new ArrayList<DDlogRelation>(), new ArrayList<DDlogRule>(),
                new ArrayList<DDlogImport>());
    }

    @Override
    public String toString() {
        String[] parts = new String[5];
        parts[0] = String.join("\n", Linq.map(this.imports, DDlogImport::toString));
        parts[1] = String.join("\n", Linq.map(this.typedefs, DDlogTypeDef::toString));
        parts[2] = String.join("\n", Linq.map(this.functions, DDlogFunction::toString));
        parts[3] = String.join("\n", Linq.map(this.relations, DDlogRelation::toString));
        parts[4] = String.join("\n", Linq.map(this.rules, DDlogRule::toString));
        return String.join("\n", parts);
    }

    public void compile() {

    }
}
