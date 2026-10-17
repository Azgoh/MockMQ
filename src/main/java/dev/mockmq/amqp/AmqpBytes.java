package dev.mockmq.amqp;

import java.nio.charset.StandardCharsets;

public final class AmqpBytes {

    private AmqpBytes() {}

    public static int readUnsignedShort(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF);
    }

    public static long readLong(byte[] bytes, int offset) {
        return ((long) (bytes[offset] & 0xFF) << 56)
                | ((long) (bytes[offset + 1] & 0xFF) << 48)
                | ((long) (bytes[offset + 2] & 0xFF) << 40)
                | ((long) (bytes[offset + 3] & 0xFF) << 32)
                | ((long) (bytes[offset + 4] & 0xFF) << 24)
                | ((long) (bytes[offset + 5] & 0xFF) << 16)
                | ((long) (bytes[offset + 6] & 0xFF) << 8)
                | ((long) (bytes[offset + 7] & 0xFF));
    }

    public static ShortString readShortString(byte[] bytes, int offset) {
        int length = bytes[offset] & 0xFF;

        int start = offset + 1;

        String value = new String(bytes, start, length, StandardCharsets.UTF_8);

        return new ShortString(value, start + length);
    }

    public record ShortString(String value, int nextOffset) {}
}
