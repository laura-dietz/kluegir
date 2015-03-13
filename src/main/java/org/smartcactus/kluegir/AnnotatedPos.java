package org.smartcactus.kluegir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
* User: dietz
* Date: 3/5/15
* Time: 5:00 PM
*/
public class AnnotatedPos<Position, FieldId> {
    public Position pos;
    public List<FieldId> fields;

    public AnnotatedPos(Position pos, List<FieldId> fields) {
        this.pos = pos;
        this.fields = fields;
    }

    public AnnotatedPos<Position,FieldId> produce() {
        return new AnnotatedPos<Position,FieldId>(pos, Collections.unmodifiableList(fields));
    }

//    public AnnotatedPos(Position pos, FieldId field) {
//        this.pos = pos;
//        this.fields = new ArrayList<FieldId>();
//        this.fields.add(field);
//    }

    @Override
    public String toString() {
        return "AnnotatedPos{" +
                "pos=" + pos +
                ", fields=" + fields +
                '}';
    }
}
