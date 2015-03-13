package org.smartcactus.kluegir;

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


    public Map<TermId, ArrayList<PostingFieldTerm<TermId, DocId, Position, FieldId>>> produceIndexCore() {
        return builder.produceIndex().index;
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