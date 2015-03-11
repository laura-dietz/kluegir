package org.smartcactus.kluegir;

import java.util.List;

/**
* User: dietz
* Date: 3/5/15
* Time: 5:00 PM
*/
public class KeyList<Key, Elem> {
    public Key term;
    public List<Elem> values;

    public KeyList(Key term, List<Elem> values) {
        this.term = term;
        this.values = values;
    }
}
