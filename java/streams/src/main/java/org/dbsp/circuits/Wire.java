package org.dbsp.circuits;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A wire connects an operator output to one or more operator inputs.
 */
public class Wire implements Consumer {
    private final List<Consumer> consumers = new ArrayList<>();

    /**
     * Current value on the wire.  If 'null' the wire has no value.
     */
    @Nullable
    public Object value;

    /**
     * Receive a value and sent it to all consumers.
     * @param val  Value to receive.
     */
    @Override
    public void receive(Object val) {
        if (this.value != null)
            throw new RuntimeException("Wire already has a value:" + this.value + " when receiving " + val);
        this.value = val;
        for (Consumer o: this.consumers)
            o.receive(value);
        this.value = null;
    }

    /**
     * Invoked at circuit construction time.  Add an operator
     * that receives the data from this wire.
     * @param consumer  Operator that consumes the produced values.
     */
    public void addConsumer(Consumer consumer) {
        this.consumers.add(consumer);
    }
}
