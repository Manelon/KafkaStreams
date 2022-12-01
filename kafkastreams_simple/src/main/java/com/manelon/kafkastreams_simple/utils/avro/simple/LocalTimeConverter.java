package com.manelon.kafkastreams_simple.utils.avro.simple;

import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

public class LocalTimeConverter {
    public static int toAvroMillis (LocalTime time){
        return (int) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay());
    }

    public static LocalTime fromAvroMillis (int millisFromMidnight){
        return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(millisFromMidnight));
    }

    public static Long toAvroMicros(LocalTime time) {
      return TimeUnit.NANOSECONDS.toMicros(time.toNanoOfDay());
    }
    
    public static LocalTime fromAvroMicros(Long microsFromMidnight) {
      return LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(microsFromMidnight));
    }
}
