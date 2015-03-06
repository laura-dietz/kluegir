package org.smartcactus.kluegir;

import java.util.List;

/**
* User: dietz
* Date: 3/5/15
* Time: 5:00 PM
*/
public class TermPosting<TermId, DocId, Position, FieldId> {
    public TermId term;
    public List<Posting<DocId, Position, FieldId>> posting;

    public TermPosting(TermId term, List<Posting<DocId, Position, FieldId>> posting) {
        this.term = term;
        this.posting = posting;
    }

}
