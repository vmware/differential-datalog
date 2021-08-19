package org.dbsp.compute;

import javafx.util.Pair;
import org.dbsp.algebraic.Group;

/**
 * A pair group combines two groups to create a group that operates pointwise on a pair's values.
 * @param <T>   Type of left value.
 * @param <S>   Type of right value.
 */
public class PairGroup<T, S> implements Group<Pair<T, S>> {
    final Group<T> gt;
    final Group<S> gs;

    /**
     * Creates a pair group from a pair of groups.
     * @param gt  Group operating on values in the left of the pair.
     * @param gs  Group operating on values in the right of the pair.
     */
    public PairGroup(Group<T> gt, Group<S> gs) {
        this.gt = gt;
        this.gs = gs;
    }

    @Override
    public Pair<T, S> minus(Pair<T, S> data) {
        return new Pair<T, S>(this.gt.minus(data.getKey()), this.gs.minus(data.getValue()));
    }

    @Override
    public Pair<T, S> add(Pair<T, S> left, Pair<T, S> right) {
        return new Pair<T, S>(this.gt.add(left.getKey(), right.getKey()),
                this.gs.add(left.getValue(), right.getValue()));
    }

    @Override
    public Pair<T, S> zero() {
        return new Pair<T, S>(this.gt.zero(), this.gs.zero());
    }
}
