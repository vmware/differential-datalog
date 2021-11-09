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

package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.Node;
import com.vmware.ddlog.ir.*;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is a singleton pattern class
 * encoding various properties about SQL semantics.
 */
public class SqlSemantics {
    private final HashSet<String> aggregateFunctions = new HashSet<String>();
    private final HashMap<String, DDlogEBinOp.BOp> arithmeticFunctions = new HashMap<String, DDlogEBinOp.BOp>();
    private final HashMap<String, DDlogEBinOp.BOp> stringFunctions = new HashMap<String, DDlogEBinOp.BOp>();
    private final HashMap<String, DDlogEBinOp.BOp> booleanFunctions = new HashMap<String, DDlogEBinOp.BOp>();

    private SqlSemantics() {
        this.aggregateFunctions.add("count");
        this.aggregateFunctions.add("sum");
        this.aggregateFunctions.add("avg");
        this.aggregateFunctions.add("min");
        this.aggregateFunctions.add("max");
        this.aggregateFunctions.add("some");
        this.aggregateFunctions.add("any");
        this.aggregateFunctions.add("every");
        this.aggregateFunctions.add("array_agg");

        this.arithmeticFunctions.put("a_eq", DDlogEBinOp.BOp.Eq);
        this.arithmeticFunctions.put("a_neq", DDlogEBinOp.BOp.Neq);
        this.arithmeticFunctions.put("a_lt", DDlogEBinOp.BOp.Lt);
        this.arithmeticFunctions.put("a_gt", DDlogEBinOp.BOp.Gt);
        this.arithmeticFunctions.put("a_lte", DDlogEBinOp.BOp.Lte);
        this.arithmeticFunctions.put("a_gte", DDlogEBinOp.BOp.Gte);
        this.arithmeticFunctions.put("a_plus", DDlogEBinOp.BOp.Plus);
        this.arithmeticFunctions.put("a_minus", DDlogEBinOp.BOp.Minus);
        this.arithmeticFunctions.put("a_mod", DDlogEBinOp.BOp.Mod);
        this.arithmeticFunctions.put("a_times", DDlogEBinOp.BOp.Times);
        this.arithmeticFunctions.put("a_div", DDlogEBinOp.BOp.Div);
        // The following don't work for 64 bits
        // this.arithmeticFunctions.put("a_shiftr", DDlogEBinOp.BOp.ShiftR);
        // this.arithmeticFunctions.put("a_shiftl", DDlogEBinOp.BOp.ShiftL);
        this.arithmeticFunctions.put("a_band", DDlogEBinOp.BOp.BAnd);
        this.arithmeticFunctions.put("a_bor", DDlogEBinOp.BOp.BOr);
        this.arithmeticFunctions.put("a_bxor", DDlogEBinOp.BOp.BXor);

        this.stringFunctions.put("s_concat", DDlogEBinOp.BOp.Concat);
        this.stringFunctions.put("s_eq", DDlogEBinOp.BOp.Eq);
        this.stringFunctions.put("s_neq", DDlogEBinOp.BOp.Neq);

        this.booleanFunctions.put("b_eq", DDlogEBinOp.BOp.Eq);
        this.booleanFunctions.put("b_neq", DDlogEBinOp.BOp.Neq);
        this.booleanFunctions.put("b_and", DDlogEBinOp.BOp.And);
        this.booleanFunctions.put("b_or", DDlogEBinOp.BOp.Or);
        this.booleanFunctions.put("b_impl", DDlogEBinOp.BOp.Impl);
    }

    public static SqlSemantics semantics = new SqlSemantics();
    static final Pattern arrayType = Pattern.compile("ARRAY\\((.+)\\)");

    public static DDlogType createType(Node node, String sqltype, boolean mayBeNull) {
        DDlogType type = null;
        if (sqltype.equals("boolean")) {
            type = DDlogTBool.instance;
        } else if (sqltype.equals("integer") || sqltype.equals("int")) {
            type = DDlogTSigned.signed64;
        } else if (sqltype.startsWith("varchar")) {
            type = DDlogTString.instance;
        } else if (sqltype.equals("bigint")) {
            type = DDlogTInt.instance;
        } else if (sqltype.equals("real")) {
            type = DDlogTDouble.instance;
        } else if (sqltype.equals("float")) {
            type = DDlogTFloat.instance;
        } else if (sqltype.equals("date")) {
            type = new DDlogTUser(node, "Date", false);
        } else if (sqltype.equals("time")) {
            type = new DDlogTUser(null, "Time", false);
        } else if (sqltype.equals("datetime") || sqltype.equals("timestamp")) {
            type = new DDlogTUser(node, "DateTime", false);
        } else {
            Matcher m = arrayType.matcher(sqltype);
            if (m.find()) {
                String subtype = m.group(1);
                DDlogType stype = createType(node, subtype, mayBeNull);
                type = new DDlogTArray(node, stype, false);
            }
        }
        if (type == null)
            throw new TranslationException("SQL type not yet implemented: " + sqltype, node);
        return type.setMayBeNull(mayBeNull);
    }

    public boolean isAggregateFunction(String functionName) {
        return this.aggregateFunctions.contains(functionName);
    }

    /**
     * Generates a library with many functions that operate on Option values.
     */
    public DDlogProgram generateLibrary() {
        DDlogProgram result = new DDlogProgram();
        for (HashMap<String, DDlogEBinOp.BOp> h: Arrays.asList(
                this.arithmeticFunctions, this.booleanFunctions, this.stringFunctions)) {
            for (String f : h.keySet()) {
                DDlogEBinOp.BOp op = h.get(f);
                for (int i = 0; i < 4; i++) {
                    String function = f + "_";
                    DDlogType leftType;
                    DDlogType rightType;
                    DDlogType raw;
                    DDlogType withNull;
                    if (h.equals(this.stringFunctions)) {
                        raw = DDlogTString.instance;
                    } else if (op.isBoolean()) {
                        raw = DDlogTBool.instance;
                    } else {
                        raw = new DDlogTSigned(null,64, false);
                    }
                    withNull = raw.setMayBeNull(true);
                    DDlogExpression leftMatch = new DDlogEVarDecl(null,"l", raw);
                    DDlogExpression rightMatch = new DDlogEVarDecl(null,"r", raw);
                    if ((i & 1) == 1) {
                        function += "N";
                        leftType = withNull;
                        leftMatch = ExpressionTranslationVisitor.wrapSome(leftMatch, leftType);
                    } else {
                        function += "R";
                        leftType = raw;
                    }
                    if ((i & 2) == 2) {
                        function += "N";
                        rightType = withNull;
                        rightMatch = ExpressionTranslationVisitor.wrapSome(rightMatch, rightType);
                    } else {
                        function += "R";
                        rightType = raw;
                    }
                    /*
                    function add(left: Option<bigint>, right: Option<bigint>): Option<bigint> =
                    match ((left, right)) {
                        (Some{a}, Some{b}) -> Some{a + b},
                        (_, _)             -> None
                    }
                    */
                    DDlogFuncArg left = new DDlogFuncArg(null,"left", false, leftType);
                    DDlogFuncArg right = new DDlogFuncArg(null,"right", false, rightType);
                    DDlogType type = DDlogType.reduceType(leftType, rightType);
                    if (op.isComparison()) {
                        type = DDlogTBool.instance.setMayBeNull(type.mayBeNull);
                    }
                    DDlogExpression def;
                    if (i == 0) {
                        def = new DDlogEBinOp(null, op,
                                new DDlogEVar(null,"left", raw), new DDlogEVar(null,"right", raw));
                    } else {
                        def = new DDlogEMatch(null,
                                new DDlogETuple(null,
                                        new DDlogEVar(null,"left", leftType),
                                        new DDlogEVar(null,"right", rightType)),
                                Arrays.asList(
                                        new DDlogEMatch.Case(null,
                                                new DDlogETuple(null, leftMatch, rightMatch),
                                                ExpressionTranslationVisitor.wrapSome(
                                                    new DDlogEBinOp(null, op,
                                                        new DDlogEVar(null, "l", raw), new DDlogEVar(null, "r", raw)), type)),
                                        new DDlogEMatch.Case(null,
                                                new DDlogETuple(null,
                                                        new DDlogEPHolder(null),
                                                        new DDlogEPHolder(null)),
                                                new DDlogENull(null, type)))
                        );
                    }
                    DDlogFunction func = new DDlogFunction(null, function, type, def, left, right);
                    result.functions.add(func);
                }
            }
        }
        return result;
    }

    String getFunction(Node node, DDlogEBinOp.BOp op, DDlogType ltype, @Nullable DDlogType rtype) {
        HashMap<String, DDlogEBinOp.BOp> map;
        if (ltype.as(DDlogTBool.class) != null) {
            map = this.booleanFunctions;
        } else if (ltype.as(DDlogTInt.class) != null ||
                ltype.as(DDlogTBit.class) != null ||
                ltype.as(DDlogTSigned.class) != null) {
            map = this.arithmeticFunctions;
        } else {
            map = this.stringFunctions;
        }

        String suffixl = ltype.mayBeNull ? "N" : "R";
        String suffixr = rtype == null ? "" : (rtype.mayBeNull ? "N" : "R");
        for (String k: map.keySet()) {
            if (map.get(k).equals(op)) {
                return k + "_" + suffixl + suffixr;
            }
        }
        throw new TranslationException("Could not find `" + op + "` for type " + ltype.toString(), node);
    }
}
