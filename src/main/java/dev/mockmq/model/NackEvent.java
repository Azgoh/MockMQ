package dev.mockmq.model;

public record NackEvent(
        long connectionId,
        int channel,
        long deliveryTag,
        boolean multiple,
        boolean requeue
) implements AmqpEvent {
}