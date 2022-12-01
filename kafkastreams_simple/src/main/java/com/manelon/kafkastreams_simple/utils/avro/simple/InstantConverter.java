package com.manelon.kafkastreams_simple.utils.avro.simple;

import java.time.Instant;

public class InstantConverter {
    public static long toAvroMillis (Instant instant){
        return instant.toEpochMilli();
    }

    public static Instant fromAvroMillis (long millisFromEpoch) {
        return Instant.ofEpochMilli(millisFromEpoch);
    }

    public static Long toAvroMicros(Instant instant) {
      long seconds = instant.getEpochSecond();
      int nanos = instant.getNano();

      if (seconds < 0 && nanos > 0) {
        long micros = Math.multiplyExact(seconds + 1, 1_000_000L);
        long adjustment = (nanos / 1_000L) - 1_000_000;

        return Math.addExact(micros, adjustment);
      } else {
        long micros = Math.multiplyExact(seconds, 1_000_000L);

        return Math.addExact(micros, nanos / 1_000L);
      }
    }

    public static Instant fromAvroMicros(Long microsFromEpoch) {
      long epochSeconds = microsFromEpoch / (1_000_000L);
      long nanoAdjustment = (microsFromEpoch % (1_000_000L)) * 1_000L;

      return Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
    }



}
