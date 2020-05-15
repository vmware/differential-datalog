package com.vmware.ddlog.ir;

import java.util.HashMap;

/**
 * Same up to alpha-renaming.
 */
public class Alpha implements IComparePolicy {
    private final HashMap<String, String> equivalent;

    public Alpha() {
        this.equivalent = new HashMap<String, String>();
    }

    @Override
    public boolean compareRelation(String relation, String other) {
        if (this.equivalent.containsKey(relation))
            return other.equals(this.equivalent.get(relation));
        this.equivalent.put(relation, other);
        return true;
    }

    @Override
    public boolean compareLocal(String varName, String other) {
        this.equivalent.put(varName, other);
        return true;
    }

    @Override
    public void exitScope(String varName, String other) {
        this.equivalent.remove(varName);
    }

    @Override
    public boolean compareIdentifier(String var, String other) {
        if (this.equivalent.containsKey(var))
            return other.equals(this.equivalent.get(var));
        this.equivalent.put(var, other);
        return true;
    }
}
