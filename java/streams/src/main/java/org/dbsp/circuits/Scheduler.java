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
 */

package org.dbsp.circuits;

import org.dbsp.circuits.operators.Operator;

import java.util.ArrayList;

/**
 * A scheduler keeps track of the nodes that are
 * ready to run and invokes them in a FIFO order.
 * The order is important for fairness and to avoid races.
 */
public class Scheduler {
    final ArrayList<Operator> readyNodes;
    private boolean verbose = false;

    public Scheduler() {
        this.readyNodes = new ArrayList<Operator>();
    }

    public void log(String s, Object... arguments) {
        if (verbose) {
            System.out.print(s);
            for (Object o: arguments) {
                System.out.print(" ");
                System.out.print(o);
            }
            System.out.println();
        }
    }

    public void setVerbosity(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * Run all ready nodes.  Returns when the list of ready nodes is empty.
     */
    public void run() {
        while (readyNodes.size() > 0) {
            Operator first = readyNodes.remove(0);
            this.log("Running node ", first);
            first.run(this);
        }
    }

    public void addReady(Operator operator) {
        this.log("Operator is ready:", operator);
        this.readyNodes.add(operator);
    }
}
