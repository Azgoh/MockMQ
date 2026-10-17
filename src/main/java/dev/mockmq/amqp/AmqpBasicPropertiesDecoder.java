package dev.mockmq.amqp;

import java.util.LinkedHashMap;
import java.util.Map;

public final class AmqpBasicPropertiesDecoder {

    private AmqpBasicPropertiesDecoder() {}

    public static Map<String, Object> decode(byte[] bytes, int offset) {
        Map<String, Object> properties = new LinkedHashMap<>();

        int flags = AmqpBytes.readUnsignedShort(bytes, offset);

        offset += 2;

        if (isSet(flags, 15)) {
            var value = AmqpBytes.readShortString(bytes, offset);

            properties.put("contentType", value.value());

            offset = value.nextOffset();
        }

        if (isSet(flags, 14)) {
            var value = AmqpBytes.readShortString(bytes, offset);

            properties.put("contentEncoding", value.value());

            offset = value.nextOffset();
        }

        if (isSet(flags, 13)) {
            var table = AmqpFieldTableReader.readTable(bytes, offset);

            properties.put("headers", table.values());

            offset = table.nextOffset();
        }

        if (isSet(flags, 12)) {
            properties.put("deliveryMode", bytes[offset] & 0xFF);

            offset++;
        }

        if (isSet(flags, 11)) {
            properties.put("priority", bytes[offset] & 0xFF);

            offset++;
        }

        if (isSet(flags, 10)) {
            var value = AmqpBytes.readShortString(bytes, offset);

            properties.put("correlationId", value.value());

            offset = value.nextOffset();
        }

        if (isSet(flags, 9)) {
            var value = AmqpBytes.readShortString(bytes, offset);

            properties.put("replyTo", value.value());

            offset = value.nextOffset();
        }

        if (isSet(flags, 8)) {
            var value = AmqpBytes.readShortString(bytes, offset);

            properties.put("expiration", value.value());

            offset = value.nextOffset();
        }

        if (isSet(flags, 7)) {
            var value = AmqpBytes.readShortString(bytes, offset);

            properties.put("messageId", value.value());

            offset = value.nextOffset();
        }

        if (isSet(flags, 6)) {
            properties.put("timestamp", AmqpBytes.readLong(bytes, offset));

            offset += 8;
        }

        if (isSet(flags, 5)) {
            var value = AmqpBytes.readShortString(bytes, offset);

            properties.put("type", value.value());

            offset = value.nextOffset();
        }

        if (isSet(flags, 4)) {
            var value = AmqpBytes.readShortString(bytes, offset);

            properties.put("userId", value.value());

            offset = value.nextOffset();
        }

        if (isSet(flags, 3)) {
            var value = AmqpBytes.readShortString(bytes, offset);

            properties.put("appId", value.value());

            offset = value.nextOffset();
        }

        if (isSet(flags, 2)) {
            var value = AmqpBytes.readShortString(bytes, offset);

            properties.put("clusterId", value.value());
        }

        return properties;
    }

    private static boolean isSet(int flags, int bit) {
        return (flags & (1 << bit)) != 0;
    }
}
