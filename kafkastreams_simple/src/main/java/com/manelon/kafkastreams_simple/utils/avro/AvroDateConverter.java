package com.manelon.kafkastreams_simple.utils.avro;

import java.time.LocalDate;
/**
 * Avro stores the dates as epochDay (number of days from 1970/1/1)
 * It shouldn't be needed this class, LocalDate has everything we need, but just to allow the devs to know how it's don
 * Interval avro DateConverter can be find in TimeConversions.DateConversion();
 */
public class AvroDateConverter {
    /**
     * Transform a local date to an Avro date
     * @see <a href="https://avro.apache.org/docs/1.11.1/specification/#date"> avro date specification </a>
     * @param date LocalDate to transform
     * @return number of days from the unix epoch, 1 January 1970 (ISO calendar).
     */
    public static int toAvro (LocalDate date){
        return (int) date.toEpochDay();
    }

    /**
     * Transform an int contains a number of days from the unix epoch, 1 January 1970 (ISO calendar) to a local date
     * @see <a href="https://avro.apache.org/docs/1.11.1/specification/#date"> avro date specification </a>
     * @param avroDate number of days from the unix epoch, 1 January 1970 (ISO calendar)
     * @return LocalDate
     */
    public static LocalDate fromAvro (int avroDate) {
        return LocalDate.ofEpochDay( (long) avroDate);
    }


}
