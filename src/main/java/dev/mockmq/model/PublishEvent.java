package dev.mockmq.model;

import java.util.Map;

public record PublishEvent(
        long connectionId, int channel, String exchange, String routingKey, Map<String, Object> properties, byte[] body)
        implements AmqpEvent {}
