package com.manelon.kafkastreams_simple;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import com.manelon.kafkastreams_simple.utils.avro.AvroDateConverter;
import com.manelon.kafkastreams_simple.utils.avro.AvroDecimalConverter;
import com.manelon.kafkastreams_simple.utils.avro.AvroInstantConverter;
import com.manelon.kafkastreams_simple.utils.avro.AvroTimeConverter;
import com.manelon.kafkastreams_simple.utils.avro.AvroUtils;
import com.manelon.model.Vulcan;

public class AvroUtilsTest {

    private Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();
    protected static final TimeConversions.DateConversion DATE_CONVERSION = new TimeConversions.DateConversion();
    protected static final TimeConversions.TimeMillisConversion TIME_CONVERSION = new TimeConversions.TimeMillisConversion();
    protected static final TimeConversions.TimestampMillisConversion TIMESTAMP_CONVERSION = new TimeConversions.TimestampMillisConversion();

    @Test
    void should_be_able_to_serialize_BigDecimal_to_avro_decimal() {

        Vulcan vulcan = new Vulcan();
        var value = new BigDecimal("3.14");
        var bankBalance = "BankBalance";
        AvroDecimalConverter.setDecimal(vulcan, bankBalance, value);
        assertEquals(value, AvroDecimalConverter.getDecimal(vulcan, bankBalance));

        Schema decimalSchema = Vulcan.SCHEMA$.getField("BankBalance").schema();
        BigDecimal decimal = new BigDecimal(3.14).setScale(2, RoundingMode.HALF_DOWN);
        var avroDecimal = conversion.toBytes(decimal, decimalSchema, decimalSchema.getLogicalType());

        assertEquals(decimal, conversion.fromBytes(avroDecimal, decimalSchema, decimalSchema.getLogicalType()));

    }

    @Test
    public void testDateConversion() throws Exception {
        LocalDate Jan_6_1970 = LocalDate.of(1970, 1, 6); // 5
        LocalDate Jan_1_1970 = LocalDate.of(1970, 1, 1); // 0
        LocalDate Dec_27_1969 = LocalDate.of(1969, 12, 27); // -5

        assertEquals(5, AvroDateConverter.toAvro(Jan_6_1970), "6 Jan 1970 should be 5");
        assertEquals(0, AvroDateConverter.toAvro(Jan_1_1970), "1 Jan 1970 should be 0");
        assertEquals(-5, AvroDateConverter.toAvro(Dec_27_1969), "27 Dec 1969 should be -5");


        assertEquals(Jan_6_1970, AvroDateConverter.fromAvro(5), "6 Jan 1970 should be 5");
        assertEquals(Jan_1_1970, AvroDateConverter.fromAvro(0), "1 Jan 1970 should be 0");
        assertEquals(Dec_27_1969, AvroDateConverter.fromAvro(-5), "27 Dec 1969 should be -5");
   
    }

    @Test
    void should_be_support_logical_types_in_generic_records() {
        LocalDateTime birthday = LocalDateTime.of(1967, 11, 17, 0, 59);
        Instant birthdayTS = birthday.toInstant(ZoneId.systemDefault().getRules().getOffset(birthday));

        GenericRecord vulcan = new GenericRecordBuilder(Vulcan.SCHEMA$)
                .set("Name", "Spock")
                .set("BankBalance", AvroDecimalConverter.decimalToBytes(BigDecimal.valueOf(17.01), Vulcan.SCHEMA$.getField("BankBalance").schema()))
                .set("DateOfBirth", AvroDateConverter.toAvro(birthday.toLocalDate()))
                .set("TimeOfBirthMiliseconds", AvroTimeConverter.LocalTimeToAvro(Vulcan.SCHEMA$, "TimeOfBirthMiliseconds", birthday.toLocalTime()))
                .set("TimeOfBirthMicroseconds", AvroTimeConverter.LocalTimeToAvro(Vulcan.SCHEMA$, "TimeOfBirthMicroseconds", birthday.toLocalTime()))
                .set("BirthTimestamMiliseconds", AvroInstantConverter.InstantToAvro(Vulcan.SCHEMA$, "BirthTimestamMiliseconds", birthdayTS))
                .set("BirthTimestampMicroseconds", AvroInstantConverter.InstantToAvro(Vulcan.SCHEMA$, "BirthTimestampMicroseconds", birthdayTS))
                .build();

        assertEquals("Spock", vulcan.get("Name")); 
        assertEquals("17.01", AvroDecimalConverter.getDecimal(vulcan, "BankBalance").toString());
        assertEquals(birthday.toLocalDate(), AvroDateConverter.fromAvro((int)vulcan.get("DateOfBirth")));
        assertEquals(birthday.toLocalTime(), AvroTimeConverter.getLocalTime(vulcan, "TimeOfBirthMiliseconds"));
        assertEquals(birthday.toLocalTime(), AvroTimeConverter.getLocalTime(vulcan,"TimeOfBirthMicroseconds"));
        assertEquals(birthdayTS, AvroInstantConverter.getInstant(vulcan,"BirthTimestamMiliseconds"));
        assertEquals(birthdayTS, AvroInstantConverter.getInstant(vulcan, "BirthTimestampMicroseconds"));

    }

    @Test
    void should_find_logical_type_schema (){
        Schema timeMillisSchemaFronNullable = AvroUtils.getFieldLogicalType(Vulcan.SCHEMA$, "TimeOfBirthMiliseconds");
        Schema TimestampMicrosecondsFromNonNullable = AvroUtils.getFieldLogicalType(Vulcan.SCHEMA$, "BirthTimestampMicroseconds");

        assertEquals(AvroUtils.TIME_MILLIS, timeMillisSchemaFronNullable.getLogicalType().getName());
        assertEquals(AvroUtils.TIMESTAMP_MICROS, TimestampMicrosecondsFromNonNullable.getLogicalType().getName());

    }



}
