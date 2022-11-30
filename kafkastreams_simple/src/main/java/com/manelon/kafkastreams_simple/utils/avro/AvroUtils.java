package com.manelon.kafkastreams_simple.utils.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public class AvroUtils {

    public static final String DECIMAL = "decimal";
    public static final String UUID = "uuid";
    public static final String DATE = "date";
    public static final String TIME_MILLIS = "time-millis";
    public static final String TIME_MICROS = "time-micros";
    public static final String TIMESTAMP_MILLIS = "timestamp-millis";
    public static final String TIMESTAMP_MICROS = "timestamp-micros";
    public static final String LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
    public static final String LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";

    /**
     * This method returns the avro schema of the logicaltype propertie.
     * When a field is optional it has two type null, and the type of the field
     * So in case to be UNION the null type is ignored
     * 
     * @param schema    the avro object
     * @param fieldName the name of the LogicalType Field
     * @return the schema of the LogicalType
     */
    public static Schema getFieldLogicalType(Schema schema, String fieldName) {
        if (schema.getField(fieldName) == null)
            throw new IllegalArgumentException(
                    "Field name " + fieldName + " in " + schema.getFullName() + " doesn't exists");

        Schema fieldSchema = schema.getField(fieldName).schema();
        // If is union we supouse we have only two options, or null or the type
        if (Type.UNION.equals(fieldSchema.getType())) {
            for (Schema typeSchema : fieldSchema.getTypes()) {
                if (typeSchema.getLogicalType() != null)
                    return typeSchema;
            }
            throw new IllegalArgumentException("Field name " + fieldName + " in " + schema.getFullName()
                    + " has not a valid logical type field type");
        } else {
            return fieldSchema;
        }
    }

}
