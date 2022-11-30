package com.manelon.kafkastreams_simple.utils.avro;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericRecord;


public class AvroDecimalConverter {

    //Inspired by https://github.com/apache/avro/blob/master/lang/java/avro/src/test/java/org/apache/avro/TestDecimalConversion.java
    final private static Conversion<BigDecimal> DECIMAL_CONVERSION = new Conversions.DecimalConversion();

    protected static final TimeConversions.DateConversion DATE_CONVERSION = new TimeConversions.DateConversion();
    protected static final TimeConversions.TimeMillisConversion TIME_CONVERSION = new TimeConversions.TimeMillisConversion();
    protected static final TimeConversions.TimestampMillisConversion TIMESTAMP_CONVERSION = new TimeConversions.TimestampMillisConversion();

    /**
     * Avro stores the logical type decimal as ByteBuffer, this method converts the avro decimal to java BigDecimal
     * @param avro the avro record
     * @param fieldName the decimal avro's field name
     * @return Big decimal from avro's field name
     */
    public static BigDecimal getDecimal (GenericRecord avro, String fieldName) {
        Schema decimalSchema = getDecimalSchema(avro, fieldName);
        return DECIMAL_CONVERSION.fromBytes((ByteBuffer)avro.get(fieldName), decimalSchema, decimalSchema.getLogicalType());     
    }

    /**
     * Avro stores the logical type decimal as BytBuffer, this method converts a BigDecimal to an avro ByteBuffer 
     * @param value Number to convert to avro decimal type
     * @param decimalSchema avro decimal schema
     * @return ByteBuffer with the avro decimal
     */
    public static ByteBuffer decimalToBytes (BigDecimal value, Schema decimalSchema) {
        return DECIMAL_CONVERSION.toBytes(value, decimalSchema, decimalSchema.getLogicalType());
    }

    /**
     * Sets a decimal value in an avro record.
     * Avro stores the logical type decimal as ByteBuffer, this method converts the BigDecimal to the avro's ByteBuffer
     * The BigDecimal should have the same scale than the schema
     * @param avro the avro record where to set the property
     * @param fieldName the name of the property to set in the avro object
     * @param value the big decimal to be set, this should be 
     */
    public static void setDecimal (GenericRecord avro, String fieldName, BigDecimal value) {
        Schema decimalSchema = getDecimalSchema(avro, fieldName);
        avro.put(fieldName, DECIMAL_CONVERSION.toBytes(value, decimalSchema, decimalSchema.getLogicalType()));

    }

    /**
     * Sets a decimal value in an avro record updatting the scale to the same than in the schema
     * Avro stores the logical type decimal as ByteBuffer, this method converts the BigDecimal to the avro's ByteBuffer
     * @param avro the avro record where to set the property
     * @param fieldName the name of the property to set in the avro object
     * @param value the big decimal to be set, this should be
     * @param roundingMode
     */
    public static void setDecimal (GenericRecord avro, String fieldName, BigDecimal value, RoundingMode roundingMode) {
        Schema decimalSchema = getDecimalSchema(avro, fieldName);
        avro.put(fieldName, DECIMAL_CONVERSION.toBytes(value.setScale((int) decimalSchema.getObjectProp("scale"), roundingMode), decimalSchema, decimalSchema.getLogicalType()));

    }

    static private Schema getDecimalSchema(GenericRecord avro, String fieldName) {
        Schema decimalSchema = AvroUtils.getFieldLogicalType(avro.getSchema(), fieldName);

        if (!"decimal".equals(decimalSchema.getLogicalType().getName()))
            throw new IllegalArgumentException("Field name " + fieldName + " is not decimal in " + avro.getSchema().getFullName());
        return decimalSchema;
    }
    
}
