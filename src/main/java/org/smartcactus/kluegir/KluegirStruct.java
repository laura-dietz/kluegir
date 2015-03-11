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
        Map<TermId, ArrayList<Posting<DocId, Position, FieldId>>> index = builder.produceIndex().index;
        return new KluegirReadableIndex<DocId, TermId, Position, FieldId>(index, docIdComparator, positionComparator);
    }


    public static class KluegirReadableIndex<DocId, TermId, Position, FieldId> {

//        private final Map<TermId, ArrayList<Posting<DocId, Position, FieldId>>> index2;
        private final Map<TermId, ArrayList<Posting<DocId, Position, FieldId>>> index;
        private final Comparator<DocId> docIdComparator;
        private final Comparator<Position> positionComparator;


        public KluegirReadableIndex(Map<TermId, ArrayList<Posting<DocId, Position, FieldId>>> index, Comparator<DocId> docIdComparator, Comparator<Position> positionComparator) {
            this.index = index;
            this.docIdComparator = docIdComparator;
            this.positionComparator = positionComparator;
        }


        public List<KeyList<TermId, Posting<DocId, Position, FieldId>>> get(List<TermId> query) {
            List<KeyList<TermId, Posting<DocId, Position, FieldId>>> result = new ArrayList<KeyList<TermId, Posting<DocId, Position, FieldId>>>();
            for(TermId q:query){
                ArrayList<Posting<DocId, Position, FieldId>> postings = index.get(q);
                if(postings != null) {
                    result.add(new KeyList<TermId, Posting<DocId, Position, FieldId>>(q, postings));
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

        static class CachedIteratorHeap<Elem>{
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

        private List<PostingFieldTerm<TermId, DocId, Position, FieldId>> merged(List<KeyList<TermId, Posting<DocId, Position, FieldId>>> postingLists) {
            List<PostingFieldTerm<TermId, DocId, Position, FieldId>> results = new ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>();

//            Step 1: Find smallest document, and fetch all term-posting lists for that document

            List<CachedIterator<Posting<DocId, Position, FieldId>>> iterators = new ArrayList<CachedIterator<Posting<DocId, Position, FieldId>>>();
            for(KeyList<TermId, Posting<DocId, Position, FieldId>> list: postingLists){
                iterators.add(new CachedIterator<Posting<DocId, Position, FieldId>>(list._2.iterator()));
            }
            CachedIteratorHeap<Posting<DocId, Position, FieldId>> iteratorHeap =
                    new CachedIteratorHeap<Posting<DocId, Position, FieldId>>(iterators,
                            new Comparator<Posting<DocId, Position, FieldId>>() {
                                public int compare(Posting<DocId, Position, FieldId> o1, Posting<DocId, Position, FieldId> o2) {
                                    return docIdComparator.compare(o1.doc, o2.doc);
                                }
                            });


            while(true) {

                List<Pair<Integer, Posting<DocId, Position, FieldId>>> minEntries = iteratorHeap.advanceMin();

                if (minEntries == null || minEntries.isEmpty()) {
                    return results;
                }
                DocId doc = minEntries.get(0)._2.doc;


//            Step2: Transcode all term-postinglists to include term with field information
                List<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>> pList = new ArrayList<AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>>();
                for (Pair<Integer, Posting<DocId, Position, FieldId>> minEntry : minEntries) {

                    // recode postingList to include term as well as fields
                    TermId term = postingLists.get(minEntry._1)._1;
                    Posting<DocId, Position, FieldId> posting = minEntry._2;
                    for (AnnotatedPos<Position, FieldId> p : posting.positions) {
                        ArrayList<PostingFieldTerm.TermField<TermId, FieldId>> termFields = new ArrayList<PostingFieldTerm.TermField<TermId, FieldId>>();
                        for (FieldId field : p.fields) {
                            termFields.add(new PostingFieldTerm.TermField<TermId, FieldId>(field, term));
                        }
                        pList.add(new AnnotatedPos<Position, PostingFieldTerm.TermField<TermId, FieldId>>(p.pos, termFields));
                    }
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

        public List<PostingFieldTerm<TermId, DocId, Position, FieldId>> getMerged(List<TermId> query) {
            return merged(get(query));
        }
    }


    public class KluegirIndex {
        private Map<TermId, ArrayList<Posting<DocId, Position, FieldId>>> index = new HashMap<TermId, ArrayList<Posting<DocId, Position, FieldId>>>();

        public void append(TermId term, DocId doc, List<AnnotatedPos<Position, FieldId>> positions) {
            if (!index.containsKey(term)) index.put(term, new ArrayList<Posting<DocId, Position, FieldId>>());

            index.get(term).add(new Posting<DocId, Position, FieldId>(doc, positions));
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

            indexBuilder.get(term).append(pos,field);;
        }

        public void produce(KluegirIndex index){
            if(dead) throw new RuntimeException("KluegirStructBuilder for "+docId+" is already dead.");

            for(Map.Entry<TermId,PostingBuilder> entry: indexBuilder.entrySet()) {
                PostingBuilder value = entry.getValue();
                index.append(entry.getKey(), docId, (List<AnnotatedPos<Position,FieldId>>) value.produce());
            }

            dead = true;
        }

    }

    public class PostingBuilder {
        private LinkedList<AnnotatedPos<Position, FieldId>> positions = new LinkedList<AnnotatedPos<Position, FieldId>>();
        private boolean dead = false;

        public void append(Position pos, FieldId field) {
            if(dead) throw new RuntimeException("PostingBuilder is already dead.");

            if(!positions.isEmpty() && positions.getLast().pos.equals(pos)){
                positions.getLast().fields.add(field);
            } else {
                positions.add(new AnnotatedPos(pos,field));
            }
        }

        public List<AnnotatedPos<Position, FieldId>> produce() {
            if(dead) throw new RuntimeException("PostingBuilder is already dead.");

            List<AnnotatedPos<Position, FieldId>> result = positions;
            positions = new LinkedList<AnnotatedPos<Position, FieldId>>();
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