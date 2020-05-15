package com.vmware.ddlog.translator;

import com.vmware.ddlog.ir.*;

import javax.annotation.Nullable;
import java.util.HashMap;

/**
 * Part of the TranslationContext that is shared between multiple
 * translation contexts.  This contains the program and allocated
 * symbols, for example.
 */
public class TranslationState {
    private final DDlogProgram program;
    private final HashMap<String, DDlogRelationDeclaration> relations;
    public final ExpressionTranslationVisitor etv;
    /**
     * Global to the whole program.
     */
    public final SymbolTable globalSymbols;
    /**
     * Local to a query.
     */
    @Nullable
    private SymbolTable localSymbols;

    public TranslationState() {
        this.program = new DDlogProgram();
        this.program.imports.add(new DDlogImport("fp", ""));
        this.program.imports.add(new DDlogImport("time", ""));
        this.program.imports.add(new DDlogImport("sql", ""));
        this.program.imports.add(new DDlogImport("sqlop", ""));
        this.relations = new HashMap<String, DDlogRelationDeclaration>();
        this.etv = new ExpressionTranslationVisitor();
        this.localSymbols = null;
        this.globalSymbols = new SymbolTable();
    }

    @Nullable
    DDlogType resolveTypeDef(DDlogTUser type) {
        for (DDlogTypeDef t: this.program.typedefs) {
            if (t.getName().equals(type.getName()))
                return t.getType();
        }
        return null;
    }

    void add(DDlogRelationDeclaration relation) {
        this.program.relations.add(relation);
        this.relations.put(relation.getName(), relation);
    }

    String freshGlobalName(String prefix) {
        return this.globalSymbols.freshName(prefix);
    }

    String freshLocalName(String prefix) {
        assert this.localSymbols != null;
        return this.localSymbols.freshName(prefix);
    }

    void beginTranslation() {
        this.localSymbols = new SymbolTable();
    }

    void add(DDlogRule rule) {
        this.program.rules.add(rule);
    }

    void add(DDlogTypeDef tdef) {
        this.program.typedefs.add(tdef);
    }

    @Nullable
    DDlogRelationDeclaration getRelation(String name) {
        return this.relations.get(name);
    }

    DDlogProgram getProgram() {
        return this.program;
    }
}
