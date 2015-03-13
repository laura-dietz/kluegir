package org.smartcactus.kluegir.heap;

import org.smartcactus.kluegir.struct.Pair;

import java.util.List;

/**
* User: dietz
* Date: 3/13/15
* Time: 9:31 AM
*/
public interface IteratorHeap<Elem>{
    /**
     * returns the 'minimum' element from the heap, along with all its 'equal' elements. Minimum and equal as defined by the comparator.
     *
     * @return null if heap is empty (i.e. all elements are consumed)
     */
    public List<Pair<Integer, Elem>> advanceMin();
}
