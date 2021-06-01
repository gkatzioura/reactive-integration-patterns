package com.gkatzioura.arrow;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class SchemaFactory {

    public static Schema DEFAULT_SCHEMA = createDefault();

    public static Schema createDefault() {
        var strField = new Field("col1", FieldType.nullable(new ArrowType.Utf8()), null);
        var intField = new Field("col2", FieldType.nullable(new ArrowType.Int(32, true)), null);

        return new Schema(List.of(strField, intField));
    }

    public static Schema schemaWithChildren() {
        var amount = new Field("amount", FieldType.nullable(new ArrowType.Decimal(19,4,128)), null);
        var currency = new Field("currency",FieldType.nullable(new ArrowType.Utf8()), null);
        var itemField = new Field("item", FieldType.nullable(new ArrowType.Utf8()), List.of(amount,currency));

        return new Schema(List.of(itemField));
    }

    public static Schema fromJson(String jsonString) {
        try {
            return Schema.fromJSON(jsonString);
        } catch (IOException e) {
            throw new ArrowExampleException(e);
        }
    }

}
