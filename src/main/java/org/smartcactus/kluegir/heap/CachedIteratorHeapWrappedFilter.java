package org.smartcactus.kluegir.heap;

import org.smartcactus.kluegir.struct.Pair;

import java.util.List;

/**
* User: dietz
* Date: 3/13/15
* Time: 9:31 AM
*/
public class CachedIteratorHeapWrappedFilter<Elem> implements IteratorHeap<Elem>{
    private final IteratorHeap<Elem> heap;
    private final FilterFun<List<Pair<Integer, Elem>>> filter;

    public CachedIteratorHeapWrappedFilter(IteratorHeap<Elem> heap, FilterFun<List<Pair<Integer, Elem>>> filter) {
        this.heap = heap;
        this.filter = filter;
    }

    public List<Pair<Integer, Elem>> advanceMin(){
        List<Pair<Integer, Elem>> minPairs = heap.advanceMin();
        while(minPairs != null && !filter.accept(minPairs)){
            minPairs = heap.advanceMin();
        }
        return minPairs;
    }
}
