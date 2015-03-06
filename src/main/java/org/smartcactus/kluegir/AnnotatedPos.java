package org.smartcactus.kluegir;

import java.util.ArrayList;

/**
* User: dietz
* Date: 3/5/15
* Time: 5:00 PM
*/
public class AnnotatedPos<Position, FieldId> {
    public Position pos;
    public ArrayList<FieldId> fields = new ArrayList<FieldId>();

    public AnnotatedPos(Position pos, ArrayList<FieldId> fields) {
        this.pos = pos;
        this.fields = fields;
    }

    public AnnotatedPos(Position pos, FieldId field) {
        this.pos = pos;
        this.fields = new ArrayList<FieldId>();
        this.fields.add(field);
    }

    @Override
    public String toString() {
        return "AnnotatedPos{" +
                "pos=" + pos +
                ", fields=" + fields +
                '}';
    }
}
