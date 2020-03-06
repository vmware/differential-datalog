package com.vmware.ddlog.translator;

import java.util.HashSet;

/**
 * A very simple symbol table; currently maintains just names.
 */
public class SymbolTable {
    private final HashSet<String> names = new HashSet<String>();
    private int counter;

    public SymbolTable() {
        this.counter = 0;
    }

    public void addName(String name) {
        if (this.names.contains(name))
            throw new RuntimeException("Duplicate name: " + name);
        this.names.add(name);
    }

    public String freshName(String prefix) {
        String name = prefix;
        while (true) {
            if (this.names.contains(name)) {
                name = prefix + this.counter;
                this.counter++;
            } else {
                this.addName(name);
                return name;
            }
        }
    }
}
