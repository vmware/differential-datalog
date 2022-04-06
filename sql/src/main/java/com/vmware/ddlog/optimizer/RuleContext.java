/*
 * Copyright (c) 2022 VMware, Inc.
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

package com.vmware.ddlog.optimizer;

import com.vmware.ddlog.ir.*;
import com.vmware.ddlog.util.Utilities;

import java.util.*;

class RuleContext {
    final DDlogRule rule;
    final Map<String, Variable> variables;
    final boolean debug;

    RuleContext(DDlogRule rule, boolean debug) {
        this.debug = debug;
        this.rule = rule;
        this.variables = new HashMap<>();
    }

    void trace(String message) {
        if (this.debug)
            System.out.println(message);
    }

    public boolean variableExists(String var) {
        return this.variables.containsKey(var);
    }

    public Variable createVariable(String var, DDlogType type) {
        Variable result;
        if (type.is(DDlogTStruct.class)) {
            DDlogTStruct ts = type.to(DDlogTStruct.class);
            result = new StructVariable(this.rule, var, ts);
        } else {
            result = new ScalarVariable(this.rule, var, type);
        }
        this.trace("Storing in context " + var);
        this.variables.put(var, result);
        return result;
    }

    public Variable getVariable(String name) {
        return Utilities.getExists(this.variables, name);
    }

    /**
     * Visitor which scans an expression and computes all variables read and variables defined.
     * In DDlog an expression can define new variables using patterns.
     */
    class ExpressionDFNodes extends DDlogVisitor {
        ExpressionNodes nodes;

        protected ExpressionDFNodes() {
            super(true);
            this.nodes = new ExpressionNodes();
        }

        @Override
        public void postorder(DDlogEVarDecl decl) {
            // Currently we do not keep track of source edges for these nodes, but
            // the final result should be a conservative approximation, since nodes.sources will contain
            // any variables which are read, and these variables must be sources
            // for these nodes as well.
            Variable var = RuleContext.this.createVariable(decl.var, decl.getType());
            this.nodes.definitions.addAll(var.getNodes());
        }

        @Override
        public void postorder(DDlogEVar expression) {
            if (RuleContext.this.variableExists(expression.var)) {
                // A use - read the variable.
                this.nodes.sources.addAll(RuleContext.this.getAllFields(expression.var));
            } else {
                // This behaves like a new variable declaration.
                Variable var = RuleContext.this.createVariable(expression.var, expression.getType());
                this.nodes.definitions.addAll(var.getNodes());
            }
        }

        @Override
        public boolean preorder(DDlogEField field) {
            if (field.struct.is(DDlogEVar.class)) {
                DDlogEVar var = field.struct.to(DDlogEVar.class);
                Variable fb = RuleContext.this.getVariable(var.var);
                this.nodes.sources.add(fb.to(StructVariable.class).getNode(field.field));
                return false;
            }

            // Ignore the field and just visit the sources
            return true;
        }
    }

    private Collection<DFNode> getAllFields(String name) {
        Variable var = Utilities.getExists(this.variables, name);
        return var.getNodes();
    }

    public ExpressionNodes expressionNodes(DDlogExpression expression) {
        ExpressionDFNodes scan = new ExpressionDFNodes();
        expression.accept(scan);
        return scan.nodes;
    }
}
