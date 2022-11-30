package com.manelon.kafkastreams_simple.utils.avro;

import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericRecord;




//see https://github.com/apache/avro/blob/master/lang/java/avro/src/main/java/org/apache/avro/data/TimeConversions.java
public class AvroInstantConverter {
    protected static final TimeConversions.TimestampMillisConversion TIMESTAMP_MILIS_CONVERSION = new TimeConversions.TimestampMillisConversion();
    protected static final TimeConversions.TimestampMicrosConversion TIMESTAMP_MICROS_CONVERSION = new TimeConversions.TimestampMicrosConversion();
    
    /**
     * Returns the Instant from a avro record
     * @param avro avro record
     * @param fieldName fieldName of logical type TIMESTAMP_MILIS or TIMESTAMP_MICROS
     * @return instant from avro record
     */
    public static Instant getInstant (GenericRecord avro, String fieldName) {
        Schema timeSchema = AvroUtils.getFieldLogicalType(avro.getSchema(), fieldName);
        
        switch(timeSchema.getLogicalType().getName()){
            case (AvroUtils.TIMESTAMP_MILLIS):
                return TIMESTAMP_MILIS_CONVERSION.fromLong((long)avro.get(fieldName), timeSchema, timeSchema.getLogicalType());
            case (AvroUtils.TIMESTAMP_MICROS):
                return TIMESTAMP_MICROS_CONVERSION.fromLong((long) avro.get(fieldName), timeSchema, timeSchema.getLogicalType());
            default:
                throw new IllegalArgumentException(
                    "The Field " + fieldName + " is not an Avro Timestamp field");
        }
        
    } 

    /**
     * Sets Instant to a field in an avro record
     * @param avro avro record
     * @param fieldName fieldName of logical type TIMESTAMP_MILIS or TIMESTAMP_MICROS
     * @param value Instant value
     */
    public static void setInstant (GenericRecord avro, String fieldName, Instant value) {
        Schema timeSchema = AvroUtils.getFieldLogicalType(avro.getSchema(), fieldName);
        
        switch(timeSchema.getLogicalType().getName()){
            case (AvroUtils.TIMESTAMP_MILLIS):
                avro.put(fieldName, TIMESTAMP_MILIS_CONVERSION.toLong(value, timeSchema, timeSchema.getLogicalType())); 
            case (AvroUtils.TIME_MICROS):
                avro.put(fieldName, TIMESTAMP_MICROS_CONVERSION.toLong(value, timeSchema, timeSchema.getLogicalType())); 
            default:
                throw new IllegalArgumentException(
                    "The Field " + fieldName + " is not an Avro Timestamp field");
        }
    }

     /**
     * Returns an avro representantion of an Instant
     * @param schema avro schema
     * @param fieldName fieldName of logical type TIMESTAMP_MILIS or TIMESTAMP_MICROS
     * @param value an instant 
     * @return a long that stores the number of microseconds or milliseconds from the unix epoch, 1 January 1970 00:00:00 UTC
     * @see <a href="https://avro.apache.org/docs/1.11.1/specification/#timestamp-millisecond-precision">the avro specification</a>
     */
    public static long InstantToAvro (Schema schema, String fieldName, Instant value) {
        Schema timeSchema = AvroUtils.getFieldLogicalType(schema, fieldName);
        
        switch(timeSchema.getLogicalType().getName()){
            case (AvroUtils.TIMESTAMP_MILLIS):
                return TIMESTAMP_MILIS_CONVERSION.toLong(value, timeSchema, timeSchema.getLogicalType()); 
            case (AvroUtils.TIMESTAMP_MICROS):
                return TIMESTAMP_MICROS_CONVERSION.toLong(value, timeSchema, timeSchema.getLogicalType()); 
            default:
                throw new IllegalArgumentException(
                    "The Field " + fieldName + " is not an Avro Timestamp field");
        }
    }    

}
