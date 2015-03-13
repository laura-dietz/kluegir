package org.smartcactus.kluegir;

import java.util.List;

/**
* User: dietz
* Date: 3/5/15
* Time: 5:00 PM
*/
public class PostingFieldTerm<TermId, DocId, Position, FieldId> {
    public DocId doc;
    public List<AnnotatedPos<Position, TermField<TermId, FieldId>>> positions;
    public int numMatches;

    public static class TermField<TermId, FieldId> {
        public TermId termId;
        public FieldId fieldId;

        public TermField(FieldId fieldId, TermId termId) {
            this.fieldId = fieldId;
            this.termId = termId;
        }

        @Override
        public String toString() {
            return "TermField{" +
                    "termId=" + termId +
                    ", fieldId=" + fieldId +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TermField termField = (TermField) o;

            if (fieldId != null ? !fieldId.equals(termField.fieldId) : termField.fieldId != null) return false;
            if (termId != null ? !termId.equals(termField.termId) : termField.termId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = termId != null ? termId.hashCode() : 0;
            result = 31 * result + (fieldId != null ? fieldId.hashCode() : 0);
            return result;
        }
    }

    public PostingFieldTerm(DocId doc, List<AnnotatedPos<Position,TermField<TermId, FieldId>>> positions) {
        this.doc = doc;
        this.positions = positions;

        this.numMatches = 0;
        for (AnnotatedPos<Position,TermField<TermId, FieldId>>p:positions) numMatches += p.fields.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PostingFieldTerm that = (PostingFieldTerm) o;

        if(this.numMatches != that.numMatches) return false;
        if (doc != null ? !doc.equals(that.doc) : that.doc != null) return false;
        if (positions != null ? !positions.equals(that.positions) : that.positions != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = doc != null ? doc.hashCode() : 0;
        result = 31 * result + (positions != null ? positions.hashCode() : 0);
        return result;
    }
}
