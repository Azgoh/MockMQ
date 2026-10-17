package dev.mockmq.tracking;

import dev.mockmq.model.AmqpEvent;

public interface AmqpEventSink {

    void accept(AmqpEvent event);
}
