package com.manelon.kafkastreams_simple.utils.avro.simple;

import java.time.LocalDate;

/**
 * Conversions without validation
 * This conversions will be fasters but it will not check anything
 */
public class LocalDateConverter {
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
