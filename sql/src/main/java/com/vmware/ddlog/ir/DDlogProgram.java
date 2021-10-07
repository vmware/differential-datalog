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

import com.vmware.ddlog.util.Linq;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class DDlogProgram extends DDlogNode {
    // We are missing some of the fields that can never be generated from SQL
    public final List<DDlogTypeDef> typedefs;
    public final List<DDlogFunction> functions;
    public final List<DDlogRelationDeclaration> relations;
    public final List<DDlogIndexDeclaration> indexes;
    public final List<DDlogRule> rules;
    public final List<DDlogImport> imports;

    DDlogProgram(List<DDlogTypeDef> typedefs, List<DDlogFunction> functions, List<DDlogRelationDeclaration> relations,
                 List<DDlogIndexDeclaration> indexes,
                 List<DDlogRule> rules, List<DDlogImport> imports) {
        super(null);
        this.typedefs = typedefs;
        this.functions = functions;
        this.relations = relations;
        this.indexes = indexes;
        this.rules = rules;
        this.imports = imports;
    }

    public DDlogProgram() {
        this(new ArrayList<DDlogTypeDef>(), new ArrayList<DDlogFunction>(),
                new ArrayList<DDlogRelationDeclaration>(),
                new ArrayList<DDlogIndexDeclaration>(),
                new ArrayList<DDlogRule>(),
                new ArrayList<DDlogImport>());
    }

    @Override
    public String toString() {
        List<String> parts = new ArrayList<>();
        parts.add(String.join("\n", Linq.map(this.imports, DDlogImport::toString)) + "\n");
        parts.add(String.join("\n", Linq.map(this.typedefs, DDlogTypeDef::toString)));
        parts.add(String.join("\n", Linq.map(this.functions, DDlogFunction::toString)));
        parts.add(String.join("\n", Linq.map(this.relations, DDlogRelationDeclaration::toString)));
        parts.add(String.join("\n", Linq.map(this.rules, DDlogRule::toString)));

        // We do this to preserve the correctness of existing tests, which are sensitive to white-space
        if (this.indexes.size() != 0) {
            parts.add(String.join("\n", Linq.map(this.indexes, DDlogIndexDeclaration::toString)));
        }
        return String.join("\n", parts);
    }

    public boolean compare(DDlogProgram other, IComparePolicy policy) {
        if (this.typedefs.size() != other.typedefs.size())
            return false;
        if (this.functions.size() != other.functions.size())
            return false;
        if (this.relations.size() != other.relations.size())
            return false;
        if (this.rules.size() != other.rules.size())
            return false;
        if (this.imports.size() != other.imports.size())
            return false;
        for (int i = 0; i < this.typedefs.size(); i++)
            if (!this.typedefs.get(i).compare(other.typedefs.get(i), policy))
                return false;
        for (int i = 0; i < this.functions.size(); i++)
            if (!this.functions.get(i).compare(other.functions.get(i), policy))
                return false;
        for (int i = 0; i < this.relations.size(); i++)
            if (!this.relations.get(i).compare(other.relations.get(i), policy))
                return false;
        for (int i = 0; i < this.indexes.size(); i++)
            if (!this.indexes.get(i).compare(other.indexes.get(i), policy))
                return false;
        for (int i = 0; i < this.rules.size(); i++)
            if (!this.rules.get(i).compare(other.rules.get(i), policy))
                return false;
        for (int i = 0; i < this.imports.size(); i++)
            if (!this.imports.get(i).compare(other.imports.get(i), policy))
                return false;
        return true;
    }

    public void toFile(String filename) throws FileNotFoundException {
        try (PrintWriter out = new PrintWriter(filename)) {
            out.println(this.toString());
        }
    }
}
