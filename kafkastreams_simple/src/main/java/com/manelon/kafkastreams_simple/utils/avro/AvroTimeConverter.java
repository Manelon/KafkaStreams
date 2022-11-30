package com.manelon.kafkastreams_simple.utils.avro;

import java.time.LocalTime;

import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericRecord;

//see https://github.com/apache/avro/blob/master/lang/java/avro/src/main/java/org/apache/avro/data/TimeConversions.java
public class AvroTimeConverter {
    protected static final TimeConversions.TimeMillisConversion TIME_MILIS_CONVERSION = new TimeConversions.TimeMillisConversion();
    protected static final TimeConversions.TimeMicrosConversion TIME_MICROS_CONVERSION = new TimeConversions.TimeMicrosConversion();

    /**
     * Returns the LocalDate from a avro record
     * 
     * @param avro      avro record
     * @param fieldName fieldName of logical type TIME_MILLIS or TIME_MICROS
     * @return LocalTime from avro record
     */
    public static LocalTime getLocalTime(GenericRecord avro, String fieldName) {
        Schema timeSchema = AvroUtils.getFieldLogicalType(avro.getSchema(), fieldName);

        switch (timeSchema.getLogicalType().getName()) {
            case (AvroUtils.TIME_MILLIS):
                return TIME_MILIS_CONVERSION.fromInt((int) avro.get(fieldName), timeSchema,
                        timeSchema.getLogicalType());
            case (AvroUtils.TIME_MICROS):
                return TIME_MICROS_CONVERSION.fromLong((long) avro.get(fieldName), timeSchema,
                        timeSchema.getLogicalType());
            default:
                throw new IllegalArgumentException(
                        "The Field " + fieldName + " is not a Avro Time field");
        }

    }

    /**
     * Sets LocalTime to a field in an avro record
     * 
     * @param avro      avro record
     * @param fieldName fieldName of logical type TIME_MILLIS or TIME_MICROS
     * @param value     LocalTime value
     */
    public static void setLocalTime(GenericRecord avro, String fieldName, LocalTime value) {
        Schema timeSchema = AvroUtils.getFieldLogicalType(avro.getSchema(), fieldName);

        switch (timeSchema.getLogicalType().getName()) {
            case (AvroUtils.TIME_MILLIS):
                avro.put(fieldName, TIME_MILIS_CONVERSION.toInt(value, timeSchema, timeSchema.getLogicalType()));
            case (AvroUtils.TIME_MICROS):
                avro.put(fieldName, TIME_MILIS_CONVERSION.toLong(value, timeSchema, timeSchema.getLogicalType()));
            default:
                throw new IllegalArgumentException(
                        "The Field " + fieldName + " is not a Avro Time field");
        }
    }

    /**
     * Converts a LocalTime to avro representation
     * 
     * @param schema    avro schema
     * @param fieldName fieldName of logical type TIME_MILLIS or TIME_MICROS
     * @param value     LocalTime to v
     * @return a long or an int
     * @see <a href=
     *      "https://avro.apache.org/docs/1.11.1/specification/#time-millisecond-precision">avro
     *      specification</a>
     */
    public static Object LocalTimeToAvro(Schema schema, String fieldName, LocalTime value) {
        Schema timeSchema = AvroUtils.getFieldLogicalType(schema, fieldName);

        switch (timeSchema.getLogicalType().getName()) {
            case (AvroUtils.TIME_MILLIS):
                return TIME_MILIS_CONVERSION.toInt(value, timeSchema, timeSchema.getLogicalType());
            case (AvroUtils.TIME_MICROS):
                return TIME_MICROS_CONVERSION.toLong(value, timeSchema, timeSchema.getLogicalType());
            default:
                throw new IllegalArgumentException(
                        "The Field " + fieldName + " is not a Avro Time field");
        }
    }

    /**
     * Converts a LocalTime to avro time_millis
     * 
     * @param schema    avro schema
     * @param fieldName fieldName of logical type TIME_MILLIS
     * @param value     LocalTime
     * @return int stores the number of milliseconds after midnight, 00:00:00.000.
     * @see <a href=
     *      "https://avro.apache.org/docs/1.11.1/specification/#time-millisecond-precision">avro
     *      specification</a>
     */
    public static int LocalTimeToTimeMillis(Schema schema, String fieldName, LocalTime value) {
        Schema timeSchema = AvroUtils.getFieldLogicalType(schema, fieldName);

        switch (timeSchema.getLogicalType().getName()) {
            case (AvroUtils.TIME_MILLIS):
                return TIME_MILIS_CONVERSION.toInt(value, timeSchema, timeSchema.getLogicalType());
            default:
                throw new IllegalArgumentException(
                        "The Field " + fieldName + " is not an Avro time_millis");
        }
    }

    /**
     * Converts a LocalTime to avro time_milcros
     * 
     * @param schema    avro schema
     * @param fieldName fieldName of logical type TIME_MILCROS
     * @param value     LocalTime
     * @return long stores the number of microseconds after midnight,
     *         00:00:00.000000
     * @see <a href=
     *      "https://avro.apache.org/docs/1.11.1/specification/#time-microsecond-precision">avro
     *      specification</a>
     */
    public static long LocalTimeToTimeMicros(Schema schema, String fieldName, LocalTime value) {
        Schema timeSchema = AvroUtils.getFieldLogicalType(schema, fieldName);

        switch (timeSchema.getLogicalType().getName()) {
            case (AvroUtils.TIME_MICROS):
                return TIME_MICROS_CONVERSION.toLong(value, timeSchema, timeSchema.getLogicalType());
            default:
                throw new IllegalArgumentException(
                        "The Field " + fieldName + " is not an Avro time_micros");
        }
    }
}
