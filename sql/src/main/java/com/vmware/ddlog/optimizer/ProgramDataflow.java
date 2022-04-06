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
import com.vmware.ddlog.translator.RelationName;

import javax.annotation.Nullable;
import java.util.*;

/**
 * The ProgramDataflow class represents information about the flow of information
 * in a DDlogProgram.  It can be used to simplify programs.  The dataflow
 * is a graph where nodes are program constructs and edges are data
 * dependencies.
 */
public class ProgramDataflow extends DDlogVisitor {
    final Set<DFNode> nodes;
    final Set<DFNode> sources;
    final Set<DFNode> sinks;
    final Map<DFNode, Set<DFNode>> edges;
    final Map<DFNode, Set<DFNode>> reverseEdge;
    @Nullable
    DDlogProgram program;
    @Nullable
    RuleContext context = null;
    boolean debug = true;

    public ProgramDataflow() {
        super(true);
        this.program = null;
        this.edges = new HashMap<>();
        this.reverseEdge = new HashMap<>();
        this.nodes = new HashSet<>();
        this.sources = new HashSet<>();
        this.sinks = new HashSet<>();
    }

    void trace(String message) {
        if (this.debug)
            System.out.println(message);
    }

    private void addNode(DFNode node, boolean source, boolean sink) {
        this.trace(" Adding " +
                    (source ? "source " : "") +
                    (sink ? "sink " : "") +
                    "node " + node);
        this.nodes.add(node);
        if (source)
            this.sources.add(node);
        if (sink)
            this.sinks.add(node);
    }

    private void addEdge(DFNode source, DFNode dest) {
        this.trace(" Adding edge " + source + " -- " + dest);
        assert this.nodes.contains(source) : "Source node unknown " + source;
        assert this.nodes.contains(dest) : "Destination node unknown " + dest;
        Set<DFNode> dests = this.edges.computeIfAbsent(source, k -> new HashSet<>());
        dests.add(dest);
        Set<DFNode> srcs = this.reverseEdge.computeIfAbsent(dest, k -> new HashSet<>());
        srcs.add(source);
    }

    public String toGraphViz() {
        StringBuilder builder = new StringBuilder();
        builder.append("digraph program {\n");
        for (DFNode node: this.nodes) {
            builder.append("   ");
            node.toGraphViz(builder, this.sinks.contains(node) || this.sources.contains(node));
        }
        for (Map.Entry<DFNode, Set<DFNode>> e: this.edges.entrySet()) {
            for (DFNode d: e.getValue()) {
                builder.append("   ")
                        .append(e.getKey().getGVName())
                        .append(" -> ")
                        .append(d.getGVName())
                        .append("\n");
            }
        }
        builder.append("}\n");
        return builder.toString();
    }

    @Override
    public boolean preorder(DDlogProgram program) {
        this.program = program;
        return true;
    }

    DDlogTStruct getStructType(IDDlogHasType object) {
        DDlogType type = object.getType();
        DDlogType canon = Objects.requireNonNull(this.program).resolveType(type);
        return canon.to(DDlogTStruct.class);
    }

    @Override
    public void postorder(DDlogRelationDeclaration rel) {
        this.trace("Processing " + rel);
        DDlogTStruct st = this.getStructType(rel);
        for (DDlogField f: st.getFields()) {
            RelationFieldNode rn = new RelationFieldNode(rel.getName(), f.getName());
            this.addNode(rn, rel.role == DDlogRelationDeclaration.Role.Input,
                    rel.role == DDlogRelationDeclaration.Role.Output);
        }
    }

    void addAllEdges(Set<DFNode> sources, DFNode dest) {
        for (DFNode s: sources) {
            this.addEdge(s, dest);
        }
    }

    public boolean preorder(DDlogRule rule) {
        assert this.context == null;
        this.context = new RuleContext(rule, this.debug);
        this.trace("Processing " + rule);

        this.trace("Processing rule body");
        // Manual traversal of the body
        for (RuleBodyTerm term: rule.body)
            term.accept(this);

        RelationName relation = rule.head.relation;
        DDlogEVar headVar = rule.head.val.to(DDlogEVar.class);
        DDlogTStruct type = this.getStructType(headVar);
        for (DDlogField field: type.getFields()) {
            // Both these nodes must exist: the src was defined in the body...
            RuleVarFieldNode src = new RuleVarFieldNode(this.context.rule, headVar.var, field.getName());
            /// ... and the dest was defined in the rule declaration.
            RelationFieldNode dest = new RelationFieldNode(relation, field.getName());
            this.addEdge(src, dest);
        }

        this.trace("Done with rule " + rule);
        this.context = null;
        return false;
    }

    void addExpressionNodes(ExpressionNodes nodes) {
        nodes.sources.forEach(this::markUsed);
        nodes.definitions.forEach(n -> this.addNode(n, false, false));
    }

    @Override
    public void postorder(BodyTermLiteral lit) {
        this.trace("Processing term " + lit);
        assert this.context != null;
        assert this.program != null;
        RelationName name = lit.atom.relation;
        DDlogExpression expr = lit.atom.val;
        DDlogTStruct type = this.getStructType(expr);
        if (expr.is(DDlogEVar.class)) {
            // A term of the form R[v].  Flow one edge from
            // each relation field to a corresponding variable.
            DDlogEVar var = expr.to(DDlogEVar.class);
            boolean defined = this.context.variableExists(var.var);

            Variable v;
            if (!defined) {
                v = this.createVariable(var, type);
            } else {
                v = this.context.getVariable(var.var);
            }

            for (DDlogField field : type.getFields()) {
                DFNode source = new RelationFieldNode(name, field.getName());
                DFNode dest = v.to(StructVariable.class).getNode(field.getName());
                this.addNode(dest, false, false);
                this.addEdge(source, dest);
            }
        } else if (expr.is(DDlogEStruct.class)) {
            // A term of the form R[constructor{f = v,...}]
            DDlogEStruct est = expr.to(DDlogEStruct.class);
            for (DDlogField field : type.getFields()) {
                DFNode source = new RelationFieldNode(name, field.getName());

                DDlogExpression fieldValue = est.getFieldValue(field.getName());
                if (fieldValue.is(DDlogEVar.class)) {
                    DDlogEVar var = fieldValue.to(DDlogEVar.class);
                    if (context.variableExists(var.var)) {
                        // If variable already exists, so this is a use
                        Variable v = context.getVariable(var.var);
                        v.getNodes().forEach(this::markUsed);
                    } else {
                        // otherwise it is a definition.
                        DDlogType fieldType = this.program.resolveType(field.getType());
                        Variable v = this.createVariable(var, fieldType);
                        this.addEdge(source, v.to(ScalarVariable.class).node);
                    }
                } else {
                    // {.f = expression}.  Mark all variables in expression
                    // as sinks.
                    ExpressionNodes nodes = context.expressionNodes(fieldValue);
                    this.addExpressionNodes(nodes);
                    this.markUsed(source);
                }
            }
        } else {
            throw new RuntimeException("Not handled case " + lit);
        }
    }

    public void postorder(RuleBodyTerm term) {
        throw new RuntimeException("Not handled yet " + term.getClass());
    }

    public void postorder(BodyTermFlatMap term) {
        this.trace("Processing term " + term);
        assert this.context != null;
        assert this.program != null;
        DDlogEVar lhs = new DDlogEVar(term.getNode(), term.var, term.mapExpr.getType());
        this.assignment(lhs, term.mapExpr);
    }

    public void postorder(BodyTermGroupby term) {
        this.trace("Processing term " + term);
        assert this.context != null;
        for (String gbvar: term.rhsGroupByVars) {
            Variable var = this.context.getVariable(gbvar);
            var.getNodes().forEach(this::markUsed);
        }
        Variable dest = this.context.createVariable(term.var.var, term.var.getType());
        DFNode node = dest.to(ScalarVariable.class).node;
        this.addNode(node, false, false);

        ExpressionNodes sources = this.context.expressionNodes(term.rhsProject);
        this.addAllEdges(sources.sources, node);
    }

    void addFlow(Variable source, Variable dest) {
        if (source.is(StructVariable.class)) {
            StructVariable ssv = source.to(StructVariable.class);
            if (dest.is(StructVariable.class)) {
                StructVariable dsv = dest.to(StructVariable.class);
                assert(ssv.fields.size() == dsv.fields.size());
                for (Map.Entry<String, DFNode> node: ssv.fields.entrySet()) {
                    DFNode src = node.getValue();
                    DFNode dst = dsv.getNode(node.getKey());
                    this.addEdge(src, dst);
                }
            } else {
                throw new RuntimeException("Incompatible types " + source + "/" + dest);
            }
        } else {
            ScalarVariable ssv = source.to(ScalarVariable.class);
            if (dest.is(ScalarVariable.class)) {
                ScalarVariable dsv = dest.to(ScalarVariable.class);
                this.addEdge(ssv.node, dsv.node);
            } else {
                throw new RuntimeException("Incompatible types " + source + "/" + dest);
            }
        }
    }

    public Variable createVariable(DDlogEVar var, DDlogType type) {
        assert this.context != null;
        Variable v = this.context.createVariable(var.var, type);
        v.getNodes().forEach(n -> this.addNode(n, false, false));
        return v;
    }

    private void assignment(DDlogEVar lhs, DDlogExpression rhs) {
        assert this.context != null;
        assert this.program != null;
        DDlogType t = this.program.resolveType(rhs.getType());
        if (t.is(DDlogTStruct.class)) {
            DDlogTStruct type = t.to(DDlogTStruct.class);
            Variable lhsVar = this.createVariable(lhs, type);
            StructVariable lhsSv = lhsVar.to(StructVariable.class);
            if (rhs.is(DDlogEVar.class)) {
                DDlogEVar rhsVar = rhs.to(DDlogEVar.class);
                StructVariable rhsSv = this.context.getVariable(rhsVar.var).to(StructVariable.class);
                this.addFlow(rhsSv, lhsSv);
            } else if (rhs.is(DDlogEStruct.class)) {
                DDlogEStruct se = rhs.to(DDlogEStruct.class);
                for (DDlogField field : type.getFields()) {
                    ExpressionNodes nodes = this.context.expressionNodes(se.getFieldValue(field.getName()));
                    nodes.definitions.forEach(n -> this.addNode(n, false, false));
                    DFNode dest = lhsSv.getNode(field.getName());
                    this.addAllEdges(nodes.sources, dest);
                }
            } else {
                // Some other expression
                for (DDlogField field : type.getFields()) {
                    ExpressionNodes nodes = this.context.expressionNodes(rhs);
                    nodes.definitions.forEach(n -> this.addNode(n, false, false));
                    DFNode dest = lhsSv.getNode(field.getName());
                    this.addAllEdges(nodes.sources, dest);
                }
            }
        } else {
            Variable lhsVar = this.createVariable(lhs, t);
            ScalarVariable lhsSv = lhsVar.to(ScalarVariable.class);
            DFNode dest = lhsSv.node;
            ExpressionNodes nodes = this.context.expressionNodes(rhs);
            nodes.definitions.forEach(n -> this.addNode(n, false, false));
            this.addAllEdges(nodes.sources, dest);
        }
    }

    public void postorder(BodyTermVarDef def) {
        this.trace("Processing term " + def);
        assert this.context != null;
        assert this.program != null;
        DDlogEVar lhs = new DDlogEVar(def.getNode(), def.getVar(), def.getExprType());
        DDlogExpression rhs = def.getRhs();
        this.assignment(lhs, rhs);
    }

    public void markUsed(DFNode node) {
        this.trace("Adding sink " + node);
        this.sinks.add(node);
    }

    public void postorder(BodyTermCondition cond) {
        this.trace("Processing term " + cond);
        assert this.context != null;
        assert this.program != null;
        assert this.program.resolveType(cond.expr.getType()).is(DDlogTBool.class) :
                "Non-Boolean condition " + cond;
        ExpressionNodes nodes = context.expressionNodes(cond.expr);
        this.addExpressionNodes(nodes);
    }

    public void computeReachability() {
        Set<DFNode> unreachable = new HashSet<>(this.nodes);
        List<DFNode> worklist = new ArrayList<>(this.sinks);
        unreachable.removeAll(this.sources);

        while (!worklist.isEmpty()) {
            DFNode node = worklist.get(0);
            worklist.remove(0);
            if (unreachable.contains(node)) {
                unreachable.remove(node);
                Set<DFNode> sources = this.reverseEdge.get(node);
                if (sources != null) {
                    worklist.addAll(sources);
                }
            }
        }

        this.trace("Unreachable nodes: " + unreachable);
    }
}
