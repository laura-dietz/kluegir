package org.smartcactus.kluegir;

import org.smartcactus.kluegir.heap.*;
import org.smartcactus.kluegir.struct.Pair;

import java.util.*;

/**
* User: dietz
* Date: 3/13/15
* Time: 9:34 AM
*/
public class KluegirIndexReader<DocId, TermId, Position, FieldId> {

    private final Map<TermId, ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>> index;
    private final Comparator<DocId> docIdComparator;
    private final Comparator<Position> positionComparator;


    public KluegirIndexReader(Map<TermId, ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>> index, Comparator<DocId> docIdComparator, Comparator<Position> positionComparator) {
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



    private IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> movePolicy(IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> heap, final List<TermId> query, final Set<TermId> allTermPolicy, final Set<TermId> orTermPolicy) {
        final FilterFun<List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>>> filterFun = new AndOrPolicy<TermId, DocId, Position, FieldId>(query, allTermPolicy, orTermPolicy);

        return new CachedIteratorHeapWrappedFilter<PostingFieldTerm<TermId, DocId, Position, FieldId>>(
                heap, filterFun);
    }

    private IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> minCountPolicy(IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> heap, final List<TermId> query, final Set<TermId> terms, int minCover, int minCount) {
        final FilterFun<List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>>> filterFun = new MinCountPolicy<TermId, DocId, Position, FieldId>(query, terms, minCover, minCount);

        return new CachedIteratorHeapWrappedFilter<PostingFieldTerm<TermId, DocId, Position, FieldId>>(
                heap, filterFun);
    }

    private IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> docWhitelistPolicy1(IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> heap, final Set<DocId> docWhitelist) {
        final FilterFun<List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>>> filterFun = new DocumentFilter<TermId, DocId, Position, FieldId>(docWhitelist);

        return new CachedIteratorHeapWrappedFilter<PostingFieldTerm<TermId, DocId, Position, FieldId>>(
                heap, filterFun);
    }

    private IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> docWhitelistPolicy(final IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> heap, final Set<DocId> docWhitelist) {
        final ArrayList<DocId> whiteOrdered = new ArrayList<DocId>();
        whiteOrdered.addAll(docWhitelist);
        Collections.sort(whiteOrdered, docIdComparator);
        final CachedIterator<DocId> whiteIterator = new CachedIterator<DocId>(whiteOrdered.iterator());

        final FilterFun<List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>>> filterFun = new DocumentFilter<TermId, DocId, Position, FieldId>(docWhitelist);

        return new IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>>(){
            public Comparator<PostingFieldTerm<TermId, DocId, Position, FieldId>> getComparator() {
                return heap.getComparator();
            }

            public List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>> advanceMin() {
                if(whiteIterator.hasNext()){
                    final DocId value = whiteIterator.next();
                    final PostingFieldTerm<TermId, DocId, Position, FieldId> wrappedValue = wrapValue(value);
                    heap.skipLessThan(wrappedValue);
                    final List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>> min = heap.advanceMin();
                    System.out.println("docSkip returning min = " + min);
                    return min;
                } else {
                    return null;
                }
            }

            private PostingFieldTerm<TermId, DocId, Position, FieldId> wrapValue(DocId value) {
                return new PostingFieldTerm<TermId, DocId, Position, FieldId>(value, Collections.<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>>emptyList());
            }

            public void skipLessThan(PostingFieldTerm<TermId, DocId, Position, FieldId> value) {
                // move the document iterator forward too
                while(getComparator().compare(wrapValue(whiteIterator.head), value)<0) {
                    System.out.println("Move whiteIterator over " + whiteIterator.head);
                    whiteIterator.next();
                }
                heap.skipLessThan(value);
            }
        };

    }

    private List<PostingFieldTerm<TermId, DocId, Position, FieldId>> merged(IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> iteratorHeap, List<TermId> query) {
        List<PostingFieldTerm<TermId, DocId, Position, FieldId>> results = new ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>();


        while(true) {

            List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>> minEntries = iteratorHeap.advanceMin();

            if (minEntries == null || minEntries.isEmpty()) {
                return results;
            }
            DocId doc = minEntries.get(0)._2.doc;


            List<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>> pList = new ArrayList<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>>();
            for (Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>> minEntry : minEntries) {

                PostingFieldTerm<TermId, DocId, Position, FieldId> posting = minEntry._2;
                pList.addAll(posting.positions);
                // todo merge-sort
            }

            Collections.sort(pList, new Comparator<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>>() {
                public int compare(AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>> o1, AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>> o2) {
                    return positionComparator.compare(o1.pos, o2.pos);
                }
            });

            pList = mergePositions(pList);
            PostingFieldTerm<TermId, DocId, Position, FieldId> termPosting = new PostingFieldTerm<TermId, DocId, Position, FieldId>(doc, pList);
            results.add(termPosting);
        }
    }

    private List<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>> mergePositions(List<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>> pList){
        LinkedList<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>> newPList = new LinkedList<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>>();
        AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>> previous = null;
        AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>> previousBuilder = null;

        for(ListIterator<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>> liter = pList.listIterator(); liter.hasNext();){
            AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>> me = liter.next();
            if(previous != null){
                if(positionComparator.compare(previous.pos, me.pos)==0){
                    if(previousBuilder == null) {
                        previousBuilder = new AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>(previous.pos, new ArrayList<PostingFieldTerm.TermField<TermId, FieldId>>());
                        previousBuilder.fields.addAll(previous.fields);
                    }
                    previousBuilder.fields.addAll(me.fields);
                } else {
                    if(previousBuilder != null) {
                        newPList.add(previousBuilder.produce());
                        previousBuilder = null;
                    }
                    else newPList.add(previous);
                    previous = me;
                }
            }  else {
                previous = me;
            }


        }
        if(previousBuilder != null) {
            newPList.add(previousBuilder);
            previousBuilder = null;
        }
        else newPList.add(previous);
        return Collections.unmodifiableList(newPList);
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

    public List<PostingFieldTerm<TermId, DocId, Position, FieldId>> getMerged(List<TermId> query, Set<TermId> andPolicy, Set<TermId> orPolicy, Set<DocId> docWhitelist, int queryMinCover, int queryMinMatches) {
        // initialize iteratorHeap
        final List<KeyList<TermId, PostingFieldTerm<TermId, DocId, Position, FieldId>>> postingLists = get(query);
        IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> iteratorHeap = setupPostingListHeap(postingLists);

        // document id whitelist
        IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> docFilteredHeap =  iteratorHeap;
        if(!docWhitelist.isEmpty()) docFilteredHeap = docWhitelistPolicy(iteratorHeap, docWhitelist);

        // filter documents that lack requires terms
        IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> andOrFilteredHeap =docFilteredHeap;
        if(!andPolicy.isEmpty() || orPolicy.isEmpty()) andOrFilteredHeap = movePolicy(docFilteredHeap, query, andPolicy, orPolicy);

        // filter documents without enough matches (cover and count)
        IteratorHeap<PostingFieldTerm<TermId, DocId, Position, FieldId>> minCountFilteredHeap =andOrFilteredHeap;
        if(queryMinCover>0 && queryMinMatches>0) minCountFilteredHeap = minCountPolicy(andOrFilteredHeap, query, new HashSet<TermId>(query), queryMinCover, queryMinMatches);

        // merge term-wise posting lists into one
        final List<PostingFieldTerm<TermId, DocId, Position, FieldId>> mergedList = merged(minCountFilteredHeap, query);
        return mergedList;
    }

    public static class AndOrPolicy<TermId, DocId, Position, FieldId> implements FilterFun<List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>>> {

        private final List<TermId> query;
        private final Set<TermId> allTermPolicy;
        private final Set<TermId> orTermPolicy;

        public AndOrPolicy(List<TermId> query, Set<TermId> allTermPolicy, Set<TermId> orTermPolicy) {
            this.query = query;
            this.allTermPolicy = allTermPolicy;
            this.orTermPolicy = orTermPolicy;
        }

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
    }

    public static class MinCountPolicy<TermId, DocId, Position, FieldId> implements FilterFun<List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>>> {

        private final List<TermId> query;
        private final Set<TermId> terms;
        private final int minCover;
        private final int minCount;

        public MinCountPolicy(List<TermId> query, Set<TermId> terms, int minCover, int minCount) {
            this.query = query;
            this.terms = terms;
            this.minCover = minCover;
            this.minCount = minCount;
        }

        public boolean accept(List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>> postingsforDoc) {
            int foundCover = 0;
            int foundCount = 0;
            for (Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>> pair : postingsforDoc) {
                final TermId termId = query.get(pair._1);
                if (terms.contains(termId)) {
                    foundCover++;
                    final int matches = pair._2.numMatches;
                    foundCount += matches;
                }

                if (foundCover > minCover && foundCount >= minCount) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class DocumentFilter<TermId, DocId, Position, FieldId> implements FilterFun<List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>>> {

        private final Set<DocId> docWhitelist;

        public DocumentFilter(Set<DocId> docWhitelist) {

            this.docWhitelist = docWhitelist;
        }

        public boolean accept(List<Pair<Integer, PostingFieldTerm<TermId, DocId, Position, FieldId>>> postingsforDoc) {

            return !postingsforDoc.isEmpty() && docWhitelist.contains(postingsforDoc.get(0)._2.doc);
        }
    }
}
