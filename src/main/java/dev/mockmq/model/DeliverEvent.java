package dev.mockmq.model;

import java.util.Map;

public record DeliverEvent(
        long connectionId,
        int channel,
        long deliveryTag,
        String exchange,
        String routingKey,
        boolean redelivered,
        Map<String, Object> properties,
        byte[] body
) implements AmqpEvent {
}