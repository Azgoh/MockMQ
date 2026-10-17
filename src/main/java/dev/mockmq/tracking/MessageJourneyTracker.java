package dev.mockmq.tracking;

import dev.mockmq.model.AmqpEvent;
import dev.mockmq.model.DeliverEvent;
import dev.mockmq.model.NackEvent;
import dev.mockmq.model.PublishEvent;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageJourneyTracker implements AmqpEventSink {

    private static final Logger logger = LoggerFactory.getLogger(MessageJourneyTracker.class);

    /*
     * fingerprint -> journey
     */
    private final Map<String, MessageJourney> journeys = new HashMap<>();

    /*
     * delivery identity -> fingerprint
     *
     * A Nack only gives us:
     * connection + channel + deliveryTag.
     *
     * So when we see Deliver first, we remember
     * which message fingerprint that delivery belongs to.
     */
    private final Map<DeliveryKey, String> deliveries = new HashMap<>();

    @Override
    public synchronized void accept(AmqpEvent event) {
        switch (event) {
            case PublishEvent publish -> onPublish(publish);

            case DeliverEvent deliver -> onDeliver(deliver);

            case NackEvent nack -> onNack(nack);
        }
    }

    private void onPublish(PublishEvent event) {
        String fingerprint = fingerprint(event.exchange(), event.routingKey(), event.body());

        MessageJourney journey = journeys.computeIfAbsent(fingerprint, MessageJourney::new);

        journey.publishCount++;

        logger.info(
                "[journey={}] PUBLISH | conn={}/ch={} | {} -> {} | body=\"{}\"",
                fingerprint,
                event.connectionId(),
                event.channel(),
                event.exchange(),
                event.routingKey(),
                text(event.body()));
    }

    private void onDeliver(DeliverEvent event) {
        String fingerprint = fingerprint(event.exchange(), event.routingKey(), event.body());

        MessageJourney journey = journeys.computeIfAbsent(fingerprint, MessageJourney::new);

        journey.deliveryCount++;

        DeliveryKey deliveryKey = new DeliveryKey(event.connectionId(), event.channel(), event.deliveryTag());

        deliveries.put(deliveryKey, fingerprint);

        logger.info(
                "[journey={}] DELIVER | conn={}/ch={} | tag={} | delivery#={} | redelivered={} | body=\"{}\"",
                fingerprint,
                event.connectionId(),
                event.channel(),
                event.deliveryTag(),
                journey.deliveryCount,
                event.redelivered(),
                text(event.body()));

        Object headers = event.properties().get("headers");

        if (headers != null) {
            logger.info("[journey={}] HEADERS | {}", fingerprint, headers);
        }
    }

    private void onNack(NackEvent event) {
        DeliveryKey deliveryKey = new DeliveryKey(event.connectionId(), event.channel(), event.deliveryTag());

        String fingerprint = deliveries.get(deliveryKey);

        if (fingerprint == null) {
            logger.info(
                    "[unmatched] NACK | conn={}/ch={} | tag={} | multiple={} | requeue={}",
                    event.connectionId(),
                    event.channel(),
                    event.deliveryTag(),
                    event.multiple(),
                    event.requeue());

            return;
        }

        MessageJourney journey = journeys.get(fingerprint);

        if (journey != null) {
            journey.nackCount++;
        }

        logger.info(
                "[journey={}] NACK | conn={}/ch={} | tag={} | nack#={} | multiple={} | requeue={}",
                fingerprint,
                event.connectionId(),
                event.channel(),
                event.deliveryTag(),
                journey != null ? journey.nackCount : "?",
                event.multiple(),
                event.requeue());
    }

    private String fingerprint(String exchange, String routingKey, byte[] body) {
        return MessageFingerprint.of(exchange, routingKey, body);
    }

    private String text(byte[] body) {
        return new String(body, StandardCharsets.UTF_8);
    }

    private record DeliveryKey(long connectionId, int channel, long deliveryTag) {}

    private static class MessageJourney {

        private final String fingerprint;

        private int publishCount;
        private int deliveryCount;
        private int nackCount;

        private MessageJourney(String fingerprint) {
            this.fingerprint = fingerprint;
        }
    }
}
