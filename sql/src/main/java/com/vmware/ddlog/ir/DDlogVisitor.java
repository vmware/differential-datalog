/*
 * Copyright (c) 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/ovoid sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies ovoid substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOvoid A PARTICULAvoid PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS Ovoid COPYRIGHT HOLDERS BE LIABLE FOvoid ANY CLAIM, DAMAGES Ovoid OTHER
 * LIABILITY, WHETHEvoid IN AN ACTION OF CONTRACT, TORT Ovoid OTHERWISE, ARISING FROM,
 * OUT OF Ovoid IN CONNECTION WITH THE SOFTWARE Ovoid THE USE Ovoid OTHEvoid DEALINGS IN THE
 * SOFTWARE.
 *
 */

package com.vmware.ddlog.ir;

/// Depth first traversal of the IR tree data structure.
public abstract class DDlogVisitor {
    /// If true each visit call will visit by default the superclass.
    boolean visitSuper;

    protected DDlogVisitor(boolean visitSuper) {
        this.visitSuper = visitSuper;
    }

    // preorder methods return 'true' when normal traversal is desired,
    // and 'false' when the traversal should stop right away at the current node.
    // base classes
    protected boolean preorder(DDlogNode ignored) {
        return true;
    }

    protected boolean preorder(RuleBodyTerm node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    protected boolean preorder(DDlogExpression node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    protected boolean preorder(DDlogType node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    // Visit concrete classes
    // Rule body terms
    public boolean preorder(BodyTermAggregate node) {
        if (this.visitSuper) return this.preorder((RuleBodyTerm) node);
        else return true;
    }

    public boolean preorder(BodyTermCondition node) {
        if (this.visitSuper) return this.preorder((RuleBodyTerm) node);
        else return true;
    }

    public boolean preorder(BodyTermFlatMap node) {
        if (this.visitSuper) return this.preorder((RuleBodyTerm) node);
        else return true;
    }

    public boolean preorder(BodyTermGroupby node) {
        if (this.visitSuper) return this.preorder((RuleBodyTerm) node);
        else return true;
    }

    public boolean preorder(BodyTermLiteral node) {
        if (this.visitSuper) return this.preorder((RuleBodyTerm) node);
        else return true;
    }

    public boolean preorder(BodyTermVarDef node) {
        if (this.visitSuper) return this.preorder((BodyTermCondition) node);
        else return true;
    }

    // Expressions
    public boolean preorder(DDlogEApply node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEAs node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEBinOp node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEBit node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEBool node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEComma node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEDouble node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEField node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEFloat node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEFor node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEInRelation node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEInt node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEIString node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEITE node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEMatch node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogENull node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEnvironment node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEPHolder node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogESeq node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogESet node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogESigned node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEString node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEStruct node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogETupField node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogETuple node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogETyped node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEUnOp node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEVar node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    public boolean preorder(DDlogEVarDecl node) {
        if (this.visitSuper) return this.preorder((DDlogExpression) node);
        else return true;
    }

    // types
    public boolean preorder(DDlogTAny node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTArray node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTBit node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTBool node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTDouble node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTFloat node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTGroup node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTInt node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTIString node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTRef node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTSet node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTSigned node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTString node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTStruct node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTTuple node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTUnknown node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTUser node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    public boolean preorder(DDlogTVar node) {
        if (this.visitSuper) return this.preorder((DDlogType) node);
        else return true;
    }

    // Other classes
    public boolean preorder(DDlogAtom node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(DDlogComment node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(DDlogField node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(DDlogFuncArg node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(DDlogFunction node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(DDlogImport node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(DDlogIndexDeclaration node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(DDlogKeyExpr node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(DDlogProgram node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(DDlogRelationDeclaration node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(DDlogRule node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(DDlogTypeDef node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    public boolean preorder(RuleBody node) {
        if (this.visitSuper) return this.preorder((DDlogNode) node);
        else return true;
    }

    // base classes
    protected void postorder(DDlogNode unused) {
    }

    protected void postorder(RuleBodyTerm node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    protected void postorder(DDlogExpression node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    protected void postorder(DDlogType node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    // Visit concrete classes
    // Rule body terms
    public void postorder(BodyTermAggregate node) {
        if (this.visitSuper) this.postorder((RuleBodyTerm) node);
    }

    public void postorder(BodyTermCondition node) {
        if (this.visitSuper) this.postorder((RuleBodyTerm) node);
    }

    public void postorder(BodyTermFlatMap node) {
        if (this.visitSuper) this.postorder((RuleBodyTerm) node);
    }

    public void postorder(BodyTermGroupby node) {
        if (this.visitSuper) this.postorder((RuleBodyTerm) node);
    }

    public void postorder(BodyTermLiteral node) {
        if (this.visitSuper) this.postorder((RuleBodyTerm) node);
    }

    public void postorder(BodyTermVarDef node) {
        if (this.visitSuper) this.postorder((BodyTermCondition) node);
    }

    // Expressions
    public void postorder(DDlogEApply node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEAs node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEBinOp node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEBit node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEBool node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEComma node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEDouble node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEField node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEFloat node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEFor node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEInRelation node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEInt node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEIString node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEITE node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEMatch node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogENull node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEnvironment node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEPHolder node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogESeq node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogESet node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogESigned node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEString node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEStruct node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogETupField node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogETuple node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogETyped node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEUnOp node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEVar node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    public void postorder(DDlogEVarDecl node) {
        if (this.visitSuper) this.postorder((DDlogExpression) node);
    }

    // types
    public void postorder(DDlogTAny node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTArray node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTBit node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTBool node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTDouble node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTFloat node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTGroup node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTInt node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTIString node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTRef node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTSet node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTSigned node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTString node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTStruct node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTTuple node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTUnknown node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTUser node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    public void postorder(DDlogTVar node) {
        if (this.visitSuper) this.postorder((DDlogType) node);
    }

    // Other classes
    public void postorder(DDlogAtom node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(DDlogComment node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(DDlogField node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(DDlogFuncArg node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(DDlogFunction node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(DDlogImport node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(DDlogIndexDeclaration node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(DDlogKeyExpr node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(DDlogProgram node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(DDlogRelationDeclaration node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(DDlogRule node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(DDlogTypeDef node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }

    public void postorder(RuleBody node) {
        if (this.visitSuper) this.postorder((DDlogNode) node);
    }
}
