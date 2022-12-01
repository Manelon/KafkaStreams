package com.manelon.kafkastreams_simple.utils.avro.simple;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class DecimalConverter {
    public static ByteBuffer toAvro(BigDecimal value) {
        return ByteBuffer.wrap(value.unscaledValue().toByteArray());
    }

    public static BigDecimal fromAvro(ByteBuffer value, int scale) {
        byte[] bytes = new byte[value.remaining()];
        value.duplicate().get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }
}
