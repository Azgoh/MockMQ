package dev.mockmq.amqp;

import dev.mockmq.model.DeliverEvent;
import dev.mockmq.model.PublishEvent;
import dev.mockmq.tracking.AmqpEventSink;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpContentTracker {

    private static final Logger logger = LoggerFactory.getLogger(AmqpContentTracker.class);

    private final long connectionId;
    private final AmqpEventSink eventSink;

    private final Map<Integer, PendingContent> pendingByChannel = new HashMap<>();

    public AmqpContentTracker(long connectionId, AmqpEventSink eventSink) {
        this.connectionId = connectionId;

        this.eventSink = eventSink;
    }

    public void onMethod(
            int channel,
            int classId,
            int methodId,
            String methodName,
            String details,
            byte[] bytes,
            int argumentsOffset) {
        if (!carriesContent(classId, methodId)) {
            return;
        }

        PendingContent content = new PendingContent(classId, methodId, methodName, details);

        if (classId == 60 && methodId == 40) {

            decodePublishMetadata(content, bytes, argumentsOffset);
        }

        if (classId == 60 && methodId == 60) {

            decodeDeliveryMetadata(content, bytes, argumentsOffset);
        }

        pendingByChannel.put(channel, content);
    }

    public void onHeader(int channel, byte[] bytes, int payloadOffset, int payloadSize) {
        PendingContent content = pendingByChannel.get(channel);

        if (content == null) {
            return;
        }

        if (payloadSize < 14) {
            return;
        }

        int classId = AmqpBytes.readUnsignedShort(bytes, payloadOffset);

        long bodySize = AmqpBytes.readLong(bytes, payloadOffset + 4);

        content.expectedBodySize = bodySize;

        Map<String, Object> properties = Map.of();

        if (classId == 60) {
            properties = AmqpBasicPropertiesDecoder.decode(bytes, payloadOffset + 12);
        }

        content.properties = properties;

        logger.info(
                "CONTENT | conn={} | channel={} | {} | bodySize={} bytes | properties={}",
                connectionId,
                channel,
                content.methodName,
                bodySize,
                properties);

        if (bodySize == 0) {
            complete(channel, content);
        }
    }

    public void onBody(int channel, byte[] bytes, int payloadOffset, int payloadSize) {
        PendingContent content = pendingByChannel.get(channel);

        if (content == null) {
            return;
        }

        content.body.write(bytes, payloadOffset, payloadSize);

        if (content.body.size() >= content.expectedBodySize) {

            complete(channel, content);
        }
    }

    private void complete(int channel, PendingContent content) {
        byte[] body = content.body.toByteArray();

        if (content.classId == 60 && content.methodId == 40) {

            eventSink.accept(new PublishEvent(
                    connectionId, channel, content.exchange, content.routingKey, content.properties, body));
        }

        if (content.classId == 60 && content.methodId == 60) {

            eventSink.accept(new DeliverEvent(
                    connectionId,
                    channel,
                    content.deliveryTag,
                    content.exchange,
                    content.routingKey,
                    content.redelivered,
                    content.properties,
                    body));
        }

        pendingByChannel.remove(channel);
    }

    private void decodePublishMetadata(PendingContent content, byte[] bytes, int offset) {
        offset += 2;

        AmqpBytes.ShortString exchange = AmqpBytes.readShortString(bytes, offset);

        offset = exchange.nextOffset();

        AmqpBytes.ShortString routingKey = AmqpBytes.readShortString(bytes, offset);

        content.exchange = exchange.value();

        content.routingKey = routingKey.value();
    }

    private void decodeDeliveryMetadata(PendingContent content, byte[] bytes, int offset) {
        AmqpBytes.ShortString consumerTag = AmqpBytes.readShortString(bytes, offset);

        offset = consumerTag.nextOffset();

        content.deliveryTag = AmqpBytes.readLong(bytes, offset);

        offset += 8;

        int flags = bytes[offset] & 0xFF;

        content.redelivered = (flags & 0x01) != 0;

        offset++;

        AmqpBytes.ShortString exchange = AmqpBytes.readShortString(bytes, offset);

        offset = exchange.nextOffset();

        AmqpBytes.ShortString routingKey = AmqpBytes.readShortString(bytes, offset);

        content.exchange = exchange.value();

        content.routingKey = routingKey.value();
    }

    private boolean carriesContent(int classId, int methodId) {
        if (classId != 60) {
            return false;
        }

        return switch (methodId) {
            case 40, 60 -> true;
            default -> false;
        };
    }

    private static class PendingContent {

        private final int classId;
        private final int methodId;

        private final String methodName;
        private final String details;

        private String exchange;
        private String routingKey;

        private long deliveryTag;
        private boolean redelivered;

        private long expectedBodySize;

        private Map<String, Object> properties = Map.of();

        private final ByteArrayOutputStream body = new ByteArrayOutputStream();

        private PendingContent(int classId, int methodId, String methodName, String details) {
            this.classId = classId;

            this.methodId = methodId;

            this.methodName = methodName;

            this.details = details;
        }
    }
}
