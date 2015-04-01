package org.smartcactus.kluegir.struct;

/**
 * User: dietz
 * Date: 3/5/15
 * Time: 10:09 PM
 */
public class Pair<T1,T2> {
    public final T1 _1;
    public final T2 _2;

    public Pair(T1 _1, T2 _2){
        this._1 = _1;
        this._2 = _2;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "_1=" + _1 +
                ", _2=" + _2 +
                '}';
    }
}
