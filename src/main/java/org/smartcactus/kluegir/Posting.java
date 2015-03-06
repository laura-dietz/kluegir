package org.smartcactus.kluegir;

import java.util.List;

/**
* User: dietz
* Date: 3/5/15
* Time: 5:00 PM
*/
public class Posting<DocId, Position, FieldId> {
    public DocId doc;
    public List<AnnotatedPos<Position, FieldId>> positions;

    public Posting(DocId doc, List<AnnotatedPos<Position, FieldId>> positions) {
        this.doc = doc;
        this.positions = positions;
    }
}
