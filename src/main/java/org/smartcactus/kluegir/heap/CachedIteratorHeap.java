package org.smartcactus.kluegir.heap;

import org.smartcactus.kluegir.struct.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
* User: dietz
* Date: 3/13/15
* Time: 9:31 AM
*/
public class CachedIteratorHeap<Elem> implements IteratorHeap<Elem>{
    private final List<CachedIterator<Elem>> iterators;
    private final Comparator<Elem> cmp;
    private int minIter = 0;

    public CachedIteratorHeap(List<CachedIterator<Elem>> iterators, Comparator<Elem> cmp){
        this.iterators = iterators;
        this.cmp = cmp;
    }

    public List<Pair<Integer, Elem>> advanceMin() {
        Elem min = null;
        int minIdx = 0;
        // determine minimum element
        for(int i=0; i<iterators.size(); i++){
            if(iterators.get(i).hasNext()) {
                if (min == null || cmp.compare(min, iterators.get(i).head) > 0) {
                    min = iterators.get(i).head;
                    minIdx = i;
                }
            }
        }
        minIter = minIdx;
        if(min == null){
            // we have consumed all elements
            return null;
        }
        List<Integer> minIndexes = new ArrayList<Integer>();
        List<Pair<Integer,Elem>> minElements = new ArrayList<Pair<Integer,Elem>>();

        // find all equals
        for(int i=0; i<iterators.size(); i++){
            if(iterators.get(i).hasNext()) {
                if (cmp.compare(min, iterators.get(i).head) == 0) {
                    min = iterators.get(i).head;
                    minIndexes.add(i);
                    minElements.add(new Pair<Integer,Elem>(i,iterators.get(i).head));
                    iterators.get(i).next();
                }
            }
        }


        return minElements;
    }


}
