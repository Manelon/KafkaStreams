package com.manelon.kafkastreams_simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import com.manelon.model.Vulcan;

public class AvroUtilsTest {

    private Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();
    
    
    @Test
    void should_be_able_to_serialize_BigDecimal_to_avro_decimal () {

        Vulcan vulcan = new Vulcan();
        var value = new BigDecimal("3.14");
        var bankBalance = "BankBalance";
        AvroUtils.setDecimal(vulcan, bankBalance, value);
        assertEquals(value, AvroUtils.getDecimal(vulcan, bankBalance));


        Schema decimalSchema = Vulcan.SCHEMA$.getField("BankBalance").schema();
        BigDecimal decimal = new BigDecimal(3.14).setScale(2, RoundingMode.HALF_DOWN);
        var avroDecimal = conversion.toBytes(decimal, decimalSchema, decimalSchema.getLogicalType());

        assertEquals(decimal, conversion.fromBytes(avroDecimal, decimalSchema, decimalSchema.getLogicalType()));

    }

}
