package org.smartcactus.kluegir;

import org.smartcactus.kluegir.struct.Pair;

import java.util.*;

/**
 * User: dietz
 * Date: 3/5/15
 * Time: 1:37 PM
 */
public class KluegirStruct<DocId,TermId,Position,FieldId> {

    private KluegirIndexBuilder builder = new KluegirIndexBuilder();

    public void initDocument(DocId doc) {
        builder.initDocument(doc);
    }

    public void streamAppend(TermId term, Position pos, FieldId field) {
        builder.streamAppend(term,pos,field);
    }

    public void produceDocument() {
        builder.produceDocument();
    }

    public KluegirReadableIndex<DocId, TermId, Position, FieldId> produceIndex(Comparator<DocId> docIdComparator, Comparator<Position> positionComparator) {
        Map<TermId, ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>> index = builder.produceIndex().index;
        return new KluegirReadableIndex<DocId, TermId, Position, FieldId>(index, docIdComparator, positionComparator);
    }


    public static class KluegirReadableIndex<DocId, TermId, Position, FieldId> {

//        private final Map<TermId, ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>> index2;
        private final Map<TermId, ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>> index;
        private final Comparator<DocId> docIdComparator;
        private final Comparator<Position> positionComparator;


        public KluegirReadableIndex(Map<TermId, ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>> index, Comparator<DocId> docIdComparator, Comparator<Position> positionComparator) {
            this.index = index;
            this.docIdComparator = docIdComparator;
            this.positionComparator = positionComparator;
        }


        public List<KeyList<TermId, PostingFieldTerm<TermId, DocId, Position, FieldId>>> get(List<TermId> query) {
            List<KeyList<TermId, PostingFieldTerm<TermId, DocId, Position, FieldId>>> result = new ArrayList<KeyList<TermId, PostingFieldTerm<TermId, DocId, Position, FieldId>>>();
            for(TermId q:query){
                ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>> postings = index.get(q);
                if(postings != null) {
                    result.add(new KeyList<TermId, PostingFieldTerm<TermId, DocId, Position, FieldId>>(q, postings));
                }
            }
            return result;
        }


        static class CachedIterator<Elem>{
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

        static interface FilterFun<Elem>{
            public boolean accept(Elem elem);
        }

        static class CachedIteratorHeapWrappedFilter<Elem> implements IteratorHeap<Elem>{
            private final IteratorHeap<Elem> heap;
            private final FilterFun<List<Pair<Integer, Elem>>> filter;

            public CachedIteratorHeapWrappedFilter(IteratorHeap<Elem> heap,FilterFun<List<Pair<Integer, Elem>>> filter) {
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

        static interface IteratorHeap<Elem>{
            /**
             * returns the 'minimum' element from the heap, along with all its 'equal' elements. Minimum and equal as defined by the comparator.
             *
             * @return null if heap is empty (i.e. all elements are consumed)
             */
            public List<Pair<Integer, Elem>> advanceMin();
        }

        static class CachedIteratorHeap<Elem> implements IteratorHeap<Elem>{
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

        private IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> filter(IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> heap, final List<TermId> query, final Set<TermId> allTermPolicy, final Set<TermId> orTermPolicy) {
            final CachedIteratorHeapWrappedFilter<PostingFieldTerm<TermId, DocId, Position, FieldId>> heapWithFilter =
                    new CachedIteratorHeapWrappedFilter<PostingFieldTerm<TermId, DocId, Position, FieldId>>(
                            heap, new FilterFun<List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>>>() {

                public boolean accept(List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>> postingsforDoc) {
                    int foundAnd = 0;
                    int foundOr = 0;
                    for (Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>> pair : postingsforDoc) {
                        final TermId termId = query.get(pair._1);
                        if (allTermPolicy.contains(termId)) foundAnd++;
                        if (orTermPolicy.contains(termId)) foundOr++;

                        if (foundOr > 0 || foundAnd >= allTermPolicy.size()) {
                            return true;
                        }
                    }
                    return false;
                }
            });

            return heapWithFilter;
        }

        private List<PostingFieldTerm<TermId, DocId, Position, FieldId>> merged(IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> iteratorHeap, List<TermId> query) {
            List<PostingFieldTerm<TermId, DocId, Position, FieldId>> results = new ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>();


            while(true) {

                List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>> minEntries = iteratorHeap.advanceMin();

                if (minEntries == null || minEntries.isEmpty()) {
                    return results;
                }
                DocId doc = minEntries.get(0)._2.doc;


//            Step2: Transcode all term-postinglists to include term with field information
                List<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>> pList = new ArrayList<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>>();
                for (Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>> minEntry : minEntries) {

                    // recode postingList to include term as well as fields
                    PostingFieldTerm<TermId, DocId, Position, FieldId> posting = minEntry._2;
                    pList.addAll(posting.positions);
//                    for (AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>> p : posting.positions) {

//                        ArrayList<PostingFieldTerm.TermField<TermId, FieldId>> termFields = p.fields;
//                        pList.add(new AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>(p.pos, termFields));
                        // todo merge-sort
//                    }
                }

                Collections.sort(pList, new Comparator<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>>() {
                    public int compare(AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>> o1, AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>> o2) {
                        return positionComparator.compare(o1.pos, o2.pos);
                    }
                });

                for (int i = 0; i < pList.size() - 1; i++) {
                    AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>> p = pList.get(i);
                    AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>> p2 = pList.get(i + 1);
                    if (positionComparator.compare(p.pos, p2.pos) == 0) {
                        p.fields.addAll(p2.fields);
                        pList.remove(i + 1);
                    }
                }

                PostingFieldTerm<TermId, DocId, Position, FieldId> termPosting = new PostingFieldTerm<TermId, DocId, Position, FieldId>(doc, pList);

                results.add(termPosting);
            }
        }

        private CachedIteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> setupPostingListHeap(List<KeyList<TermId, PostingFieldTerm<TermId, DocId, Position, FieldId>>> postingLists) {
            List<CachedIterator<PostingFieldTerm<TermId, DocId, Position, FieldId>>> iterators = new ArrayList<CachedIterator<PostingFieldTerm<TermId, DocId, Position, FieldId>>>();
            for(KeyList<TermId, PostingFieldTerm<TermId, DocId, Position, FieldId>> list: postingLists){
                iterators.add(new CachedIterator<PostingFieldTerm<TermId, DocId, Position, FieldId>>(list._2.iterator()));
            }
            return new CachedIteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>>(iterators,
                    new Comparator<PostingFieldTerm<TermId, DocId, Position, FieldId>>() {
                        public int compare(PostingFieldTerm<TermId, DocId, Position, FieldId> o1, PostingFieldTerm<TermId, DocId, Position, FieldId> o2) {
                            return docIdComparator.compare(o1.doc, o2.doc);
                        }
                    });
        }

        public List<PostingFieldTerm<TermId, DocId, Position, FieldId>> getMerged(List<TermId> query, Set<TermId> andPolicy, Set<TermId> orPolicy) {
            final List<KeyList<TermId, PostingFieldTerm<TermId, DocId, Position, FieldId>>> postingLists = get(query);
            IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> iteratorHeap = setupPostingListHeap(postingLists);
            return merged(filter(iteratorHeap, query, andPolicy, orPolicy), query);
        }
    }


    public class KluegirIndex {
        private Map<TermId, ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>> index = new HashMap<TermId, ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>>();

        public void append(TermId term, DocId doc, List<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId,FieldId>>> positions) {
            if (!index.containsKey(term)) index.put(term, new ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>());

            index.get(term).add(new PostingFieldTerm<TermId, DocId, Position, FieldId>(doc, positions));
        }
    }

    // ======= Builder ============

    public class KluegirStructBuilder {
        private Map<TermId, PostingBuilder> indexBuilder;
        private DocId docId;
        private boolean dead = false;

        public KluegirStructBuilder(DocId docId){
            this.docId = docId;
            this.indexBuilder = new HashMap<TermId, PostingBuilder>();
        }

        public void streamAppend(TermId term, Position pos, FieldId field) {
            if(dead) throw new RuntimeException("KluegirStructBuilder for "+docId+" is already dead.");

            if(!indexBuilder.containsKey(term)){
                indexBuilder.put(term, new PostingBuilder());
            }

            indexBuilder.get(term).append(pos,new PostingFieldTerm.TermField<TermId,FieldId>(field, term));
        }

        public void produce(KluegirIndex index){
            if(dead) throw new RuntimeException("KluegirStructBuilder for "+docId+" is already dead.");

            for(Map.Entry<TermId,PostingBuilder> entry: indexBuilder.entrySet()) {
                index.append(entry.getKey(), docId, (List<AnnotatedPos<Position,PostingFieldTerm.TermField<TermId,FieldId>>>) entry.getValue().produce());
            }

            dead = true;
        }

    }

    public class PostingBuilder {
        private LinkedList<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId,FieldId>>> positions = new LinkedList<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId,FieldId>>>();
        private boolean dead = false;

        public void append(Position pos, PostingFieldTerm.TermField<TermId,FieldId> termfield) {
            if(dead) throw new RuntimeException("PostingBuilder is already dead.");

            if(!positions.isEmpty() && positions.getLast().pos.equals(pos)){
                positions.getLast().fields.add(termfield);
            } else {
                positions.add(new AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>(pos,termfield));
            }
        }

        public List<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId,FieldId>>> produce() {
            if(dead) throw new RuntimeException("PostingBuilder is already dead.");

            List<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId,FieldId>>> result = positions;
            positions = new LinkedList<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId,FieldId>>>();
            dead = true;
            return result;
        }

    }

    public class KluegirIndexBuilder {
        private KluegirIndex kindex = new KluegirIndex();

        private KluegirStructBuilder structBuilder = null;

        public void initDocument(DocId doc) {
            structBuilder = new KluegirStructBuilder(doc);
        }

        public void streamAppend(TermId term, Position pos, FieldId field) {
            structBuilder.streamAppend(term, pos, field);
        }

        public void produceDocument() {
            structBuilder.produce(kindex);
        }


        public KluegirIndex produceIndex() {
            KluegirIndex kindex = this.kindex;
            this.kindex = new KluegirIndex();

            return kindex;
        }

    }



}