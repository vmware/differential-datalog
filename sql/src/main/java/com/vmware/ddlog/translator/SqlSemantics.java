package com.vmware.ddlog.translator;

import com.vmware.ddlog.ir.*;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

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

    public static DDlogType createType(String sqltype, boolean mayBeNull) {
        DDlogType type = null;
        if (sqltype.equals("boolean")) {
            type = DDlogTBool.instance;
        } else if (sqltype.equals("integer")) {
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
            type = new DDlogTUser("sqlDate", false);
        } else if (sqltype.equals("time")) {
            type = new DDlogTUser("sqlTime", false);
        }
        if (type == null)
            throw new RuntimeException("SQL type not yet implemented: " + sqltype);
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
                        raw = new DDlogTSigned(64, false);
                    }
                    withNull = raw.setMayBeNull(true);
                    DDlogExpression leftMatch = new DDlogEVarDecl("l", raw);
                    DDlogExpression rightMatch = new DDlogEVarDecl("r", raw);
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
                    DDlogFuncArg left = new DDlogFuncArg("left", false, leftType);
                    DDlogFuncArg right = new DDlogFuncArg("right", false, rightType);
                    DDlogType type = DDlogType.reduceType(leftType, rightType);
                    if (op.isComparison()) {
                        type = DDlogTBool.instance.setMayBeNull(type.mayBeNull);
                    }
                    DDlogExpression def;
                    if (i == 0) {
                        def = new DDlogEBinOp(op,
                                new DDlogEVar("left", raw), new DDlogEVar("right", raw));
                    } else {
                        def = new DDlogEMatch(
                                new DDlogETuple(
                                        new DDlogEVar("left", leftType),
                                        new DDlogEVar("right", rightType)),
                                Arrays.asList(
                                        new DDlogEMatch.Case(
                                                new DDlogETuple(leftMatch, rightMatch),
                                                ExpressionTranslationVisitor.wrapSome(
                                                    new DDlogEBinOp(op,
                                                        new DDlogEVar("l", raw), new DDlogEVar("r", raw)), type)),
                                        new DDlogEMatch.Case(
                                                new DDlogETuple(
                                                        new DDlogEPHolder(),
                                                        new DDlogEPHolder()),
                                                new DDlogENull(type)))
                        );
                    }
                    DDlogFunction func = new DDlogFunction(
                            function, type, def, left, right);
                    result.functions.add(func);
                }
            }
        }
        return result;
    }

    String getFunction(DDlogEBinOp.BOp op, DDlogType ltype, @Nullable DDlogType rtype) {
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
        throw new RuntimeException("Could not find `" + op + "` for type " + ltype.toString());
    }


}
