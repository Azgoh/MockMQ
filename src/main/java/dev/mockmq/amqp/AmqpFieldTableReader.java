package dev.mockmq.amqp;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class AmqpFieldTableReader {

    private AmqpFieldTableReader() {}

    public static Result readTable(byte[] bytes, int offset) {
        int tableSize = readInt(bytes, offset);
        offset += 4;

        int end = offset + tableSize;

        Map<String, Object> values = new LinkedHashMap<>();

        while (offset < end) {
            AmqpBytes.ShortString key = AmqpBytes.readShortString(bytes, offset);

            offset = key.nextOffset();

            ValueResult value = readValue(bytes, offset);

            offset = value.nextOffset();

            values.put(key.value(), value.value());
        }

        return new Result(values, offset);
    }

    private static ValueResult readValue(byte[] bytes, int offset) {
        char type = (char) (bytes[offset] & 0xFF);
        offset++;

        return switch (type) {
            case 't' -> new ValueResult(bytes[offset] != 0, offset + 1);

            case 'b' -> new ValueResult(bytes[offset], offset + 1);

            case 'B' -> new ValueResult(bytes[offset] & 0xFF, offset + 1);

            case 's' -> new ValueResult((short) AmqpBytes.readUnsignedShort(bytes, offset), offset + 2);

            case 'u' -> new ValueResult(AmqpBytes.readUnsignedShort(bytes, offset), offset + 2);

            case 'I' -> new ValueResult(readInt(bytes, offset), offset + 4);

            case 'i' -> new ValueResult(Integer.toUnsignedLong(readInt(bytes, offset)), offset + 4);

            case 'l' -> new ValueResult(AmqpBytes.readLong(bytes, offset), offset + 8);

            case 'f' -> new ValueResult(Float.intBitsToFloat(readInt(bytes, offset)), offset + 4);

            case 'd' -> new ValueResult(Double.longBitsToDouble(AmqpBytes.readLong(bytes, offset)), offset + 8);

            case 'S' -> readLongString(bytes, offset);

            case 'A' -> readArray(bytes, offset);

            case 'T' -> new ValueResult(AmqpBytes.readLong(bytes, offset), offset + 8);

            case 'F' -> {
                Result table = readTable(bytes, offset);

                yield new ValueResult(table.values(), table.nextOffset());
            }

            case 'V' -> new ValueResult(null, offset);

            case 'x' -> readByteArray(bytes, offset);

            default ->
                throw new IllegalArgumentException(
                        "Unsupported AMQP field type: " + type + " (0x" + Integer.toHexString(type) + ")");
        };
    }

    private static ValueResult readArray(byte[] bytes, int offset) {
        int size = readInt(bytes, offset);
        offset += 4;

        int end = offset + size;

        List<Object> values = new ArrayList<>();

        while (offset < end) {
            ValueResult value = readValue(bytes, offset);

            values.add(value.value());
            offset = value.nextOffset();
        }

        return new ValueResult(values, offset);
    }

    private static ValueResult readLongString(byte[] bytes, int offset) {
        int length = readInt(bytes, offset);
        offset += 4;

        String value = new String(bytes, offset, length, StandardCharsets.UTF_8);

        return new ValueResult(value, offset + length);
    }

    private static ValueResult readByteArray(byte[] bytes, int offset) {
        int length = readInt(bytes, offset);
        offset += 4;

        byte[] value = new byte[length];

        System.arraycopy(bytes, offset, value, 0, length);

        return new ValueResult(value, offset + length);
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    public record Result(Map<String, Object> values, int nextOffset) {}

    private record ValueResult(Object value, int nextOffset) {}
}
