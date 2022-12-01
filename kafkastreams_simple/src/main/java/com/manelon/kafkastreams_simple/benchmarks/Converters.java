package com.manelon.kafkastreams_simple.benchmarks;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import com.manelon.kafkastreams_simple.utils.avro.AvroDateConverter;
import com.manelon.kafkastreams_simple.utils.avro.AvroDecimalConverter;
import com.manelon.kafkastreams_simple.utils.avro.AvroInstantConverter;
import com.manelon.kafkastreams_simple.utils.avro.AvroTimeConverter;
import com.manelon.kafkastreams_simple.utils.avro.simple.DecimalConverter;
import com.manelon.kafkastreams_simple.utils.avro.simple.InstantConverter;
import com.manelon.kafkastreams_simple.utils.avro.simple.LocalDateConverter;
import com.manelon.kafkastreams_simple.utils.avro.simple.LocalTimeConverter;
import com.manelon.model.Vulcan;

public class Converters {
    public static void main (String[] args ) throws RunnerException, IOException{

        

       Options opt = new OptionsBuilder()
                .include(Converters.class.getSimpleName())
                .warmupIterations(3)
                .forks(1)
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.NANOSECONDS)
                .addProfiler(GCProfiler.class)
                .addProfiler(StackProfiler.class)
                .build();

        new Runner(opt).run();
    }

    


    @Benchmark
    public void AvroWithVerboseConverter() {

        LocalDateTime birthday = LocalDateTime.of(1967, 11, 17, 0, 59);
		Instant birthdayTS = birthday.toInstant(ZoneId.systemDefault().getRules().getOffset(birthday));
        BigDecimal bankBalance = BigDecimal.valueOf(17.01);
        
        GenericRecord vulcan = new GenericRecordBuilder(Vulcan.SCHEMA$)
                .set("Name", "Spock")
                .set("Inteligence", 10)
                .set("BankBalance", AvroDecimalConverter.decimalToBytes(bankBalance, Vulcan.SCHEMA$.getField("BankBalance").schema()))
                .set("DateOfBirth", AvroDateConverter.toAvro(birthday.toLocalDate()))
                .set("TimeOfBirthMiliseconds", AvroTimeConverter.LocalTimeToAvro(Vulcan.SCHEMA$, "TimeOfBirthMiliseconds", birthday.toLocalTime()))
                .set("TimeOfBirthMicroseconds", AvroTimeConverter.LocalTimeToAvro(Vulcan.SCHEMA$, "TimeOfBirthMicroseconds", birthday.toLocalTime()))
                .set("BirthTimestamMiliseconds", AvroInstantConverter.InstantToAvro(Vulcan.SCHEMA$, "BirthTimestamMiliseconds", birthdayTS))
                .set("BirthTimestampMicroseconds", AvroInstantConverter.InstantToAvro(Vulcan.SCHEMA$, "BirthTimestampMicroseconds", birthdayTS))
                .build();
    }

        @Benchmark
    public void AvroWithSimpleConverter() {

        LocalDateTime birthday = LocalDateTime.of(1967, 11, 17, 0, 59);
		Instant birthdayTS = birthday.toInstant(ZoneId.systemDefault().getRules().getOffset(birthday));
        BigDecimal bankBalance = BigDecimal.valueOf(17.01);
        
        GenericRecord vulcan = new GenericRecordBuilder(Vulcan.SCHEMA$)
                .set("Name", "Spock")
                .set("Inteligence", 10)
                .set("BankBalance", DecimalConverter.toAvro(bankBalance))
                .set("DateOfBirth", LocalDateConverter.toAvro(birthday.toLocalDate()))
                .set("TimeOfBirthMiliseconds", LocalTimeConverter.toAvroMillis(birthday.toLocalTime()))
                .set("TimeOfBirthMicroseconds", LocalTimeConverter.toAvroMicros(birthday.toLocalTime()))
                .set("BirthTimestamMiliseconds", InstantConverter.toAvroMillis(birthdayTS))
                .set("BirthTimestampMicroseconds", InstantConverter.toAvroMillis(birthdayTS))
                .build();
    }
    
    
    
}
