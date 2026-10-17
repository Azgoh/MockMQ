package dev.mockmq.model;

public sealed interface AmqpEvent
        permits PublishEvent,
        DeliverEvent,
        NackEvent {

    long connectionId();

    int channel();
}