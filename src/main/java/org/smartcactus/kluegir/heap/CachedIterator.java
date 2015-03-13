package org.smartcactus.kluegir.heap;

import java.util.Iterator;

/**
* User: dietz
* Date: 3/13/15
* Time: 9:32 AM
*/
public class CachedIterator<Elem>{
    private Iterator<Elem> iterator;
    public Elem head;

    public CachedIterator(Iterator<Elem> iterator) {
        this.iterator = iterator;
        if(iterator.hasNext()) {
            head = iterator.next();
        } else head = null;
    }

    public boolean hasNext() {
        return head != null;
    }

    public Elem next() {
        Elem tmp = head;
        if(iterator.hasNext()) {
            head = iterator.next();
        } else {
            head = null;
        }
        return tmp;
    }
}
