package dev.mockmq.amqp;

import dev.mockmq.amqp.AmqpBytes.ShortString;

public final class AmqpMethodDecoder {

    private AmqpMethodDecoder() {}

    public static String decode(byte[] bytes, int classId, int methodId, int offset) {
        if (classId != 60) {
            return "";
        }

        return switch (methodId) {
            case 40 -> decodeBasicPublish(bytes, offset);
            case 60 -> decodeBasicDeliver(bytes, offset);
            case 120 -> decodeBasicNack(bytes, offset);
            default -> "";
        };
    }

    private static String decodeBasicPublish(byte[] bytes, int offset) {
        // ticket
        offset += 2;

        ShortString exchange = AmqpBytes.readShortString(bytes, offset);

        offset = exchange.nextOffset();

        ShortString routingKey = AmqpBytes.readShortString(bytes, offset);

        offset = routingKey.nextOffset();

        int flags = bytes[offset] & 0xFF;

        boolean mandatory = (flags & 0x01) != 0;

        boolean immediate = (flags & 0x02) != 0;

        return String.format(
                " | exchange=%s | routingKey=%s | mandatory=%s | immediate=%s",
                exchange.value(), routingKey.value(), mandatory, immediate);
    }

    private static String decodeBasicDeliver(byte[] bytes, int offset) {
        ShortString consumerTag = AmqpBytes.readShortString(bytes, offset);

        offset = consumerTag.nextOffset();

        long deliveryTag = AmqpBytes.readLong(bytes, offset);

        offset += 8;

        int flags = bytes[offset] & 0xFF;

        boolean redelivered = (flags & 0x01) != 0;

        offset += 1;

        ShortString exchange = AmqpBytes.readShortString(bytes, offset);

        offset = exchange.nextOffset();

        ShortString routingKey = AmqpBytes.readShortString(bytes, offset);

        return String.format(
                " | consumerTag=%s | deliveryTag=%d | redelivered=%s | exchange=%s | routingKey=%s",
                consumerTag.value(), deliveryTag, redelivered, exchange.value(), routingKey.value());
    }

    private static String decodeBasicNack(byte[] bytes, int offset) {
        long deliveryTag = AmqpBytes.readLong(bytes, offset);

        offset += 8;

        int flags = bytes[offset] & 0xFF;

        boolean multiple = (flags & 0x01) != 0;

        boolean requeue = (flags & 0x02) != 0;

        return String.format(" | deliveryTag=%d | multiple=%s | requeue=%s", deliveryTag, multiple, requeue);
    }
}
