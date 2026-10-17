package dev.mockmq.amqp;

import dev.mockmq.model.NackEvent;
import dev.mockmq.tracking.AmqpEventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class AmqpFrameParser {

    private static final Logger logger =
            LoggerFactory.getLogger(
                    AmqpFrameParser.class
            );

    private static final int FRAME_HEADER_SIZE = 7;
    private static final int FRAME_END_SIZE = 1;
    private static final int FRAME_END = 0xCE;

    private static final byte[] AMQP_PROTOCOL_HEADER = {
            'A', 'M', 'Q', 'P',
            0, 0, 9, 1
    };

    private final long connectionId;
    private final boolean expectProtocolHeader;
    private final AmqpEventSink eventSink;
    private final AmqpContentTracker contentTracker;

    private boolean protocolHeaderProcessed;

    private byte[] pending =
            new byte[0];

    public AmqpFrameParser(
            long connectionId,
            boolean expectProtocolHeader,
            AmqpEventSink eventSink
    ) {
        this.connectionId = connectionId;

        this.expectProtocolHeader =
                expectProtocolHeader;

        this.protocolHeaderProcessed =
                !expectProtocolHeader;

        this.eventSink =
                eventSink;

        this.contentTracker =
                new AmqpContentTracker(
                        connectionId,
                        eventSink
                );
    }

    public void accept(
            byte[] bytes,
            int length,
            String direction
    ) {
        append(
                bytes,
                length
        );

        if (!protocolHeaderProcessed) {
            if (!processProtocolHeader(direction)) {
                return;
            }
        }

        parseFrames(direction);
    }

    private void append(
            byte[] bytes,
            int length
    ) {
        byte[] incoming =
                Arrays.copyOf(
                        bytes,
                        length
                );

        byte[] combined =
                new byte[
                        pending.length
                                + incoming.length
                        ];

        System.arraycopy(
                pending,
                0,
                combined,
                0,
                pending.length
        );

        System.arraycopy(
                incoming,
                0,
                combined,
                pending.length,
                incoming.length
        );

        pending = combined;
    }

    private boolean processProtocolHeader(
            String direction
    ) {
        if (pending.length
                < AMQP_PROTOCOL_HEADER.length) {

            return false;
        }

        byte[] header =
                Arrays.copyOfRange(
                        pending,
                        0,
                        AMQP_PROTOCOL_HEADER.length
                );

        if (!Arrays.equals(
                header,
                AMQP_PROTOCOL_HEADER
        )) {
            logger.warn(
                    "{} | conn={} | Invalid AMQP protocol header",
                    direction,
                    connectionId
            );

            return false;
        }

        logger.info(
                "{} | conn={} | AMQP 0-9-1 protocol header",
                direction,
                connectionId
        );

        pending =
                Arrays.copyOfRange(
                        pending,
                        AMQP_PROTOCOL_HEADER.length,
                        pending.length
                );

        protocolHeaderProcessed = true;

        return true;
    }

    private void parseFrames(
            String direction
    ) {
        while (pending.length >= FRAME_HEADER_SIZE) {

            int frameType =
                    pending[0] & 0xFF;

            int channel =
                    AmqpBytes.readUnsignedShort(
                            pending,
                            1
                    );

            int payloadSize =
                    readPayloadSize();

            int fullFrameSize =
                    FRAME_HEADER_SIZE
                            + payloadSize
                            + FRAME_END_SIZE;

            if (pending.length < fullFrameSize) {
                return;
            }

            int frameEnd =
                    pending[
                            fullFrameSize - 1
                            ] & 0xFF;

            if (frameEnd != FRAME_END) {
                logger.warn(
                        "{} | conn={} | Invalid frame end: 0x{}",
                        direction,
                        connectionId,
                        Integer.toHexString(
                                frameEnd
                        )
                );

                return;
            }

            if (frameType == 1
                    && payloadSize >= 4) {

                handleMethodFrame(
                        direction,
                        channel,
                        payloadSize
                );

            } else {
                handleNonMethodFrame(
                        direction,
                        frameType,
                        channel,
                        payloadSize
                );
            }

            discardFrame(
                    fullFrameSize
            );
        }
    }

    private void handleMethodFrame(
            String direction,
            int channel,
            int payloadSize
    ) {
        int payloadOffset =
                FRAME_HEADER_SIZE;

        int classId =
                AmqpBytes.readUnsignedShort(
                        pending,
                        payloadOffset
                );

        int methodId =
                AmqpBytes.readUnsignedShort(
                        pending,
                        payloadOffset + 2
                );

        String methodName =
                AmqpMethodRegistry.name(
                        classId,
                        methodId
                );

        String details =
                AmqpMethodDecoder.decode(
                        pending,
                        classId,
                        methodId,
                        payloadOffset + 4
                );

        logger.info(
                "{} | conn={} | METHOD | channel={} | {}{} | payload={} bytes",
                direction,
                connectionId,
                channel,
                methodName,
                details,
                payloadSize
        );

        contentTracker.onMethod(
                channel,
                classId,
                methodId,
                methodName,
                details,
                pending,
                payloadOffset + 4
        );

        if (classId == 60
                && methodId == 120) {

            emitNackEvent(
                    channel,
                    payloadOffset + 4
            );
        }
    }

    private void emitNackEvent(
            int channel,
            int offset
    ) {
        long deliveryTag =
                AmqpBytes.readLong(
                        pending,
                        offset
                );

        offset += 8;

        int flags =
                pending[offset] & 0xFF;

        boolean multiple =
                (flags & 0x01) != 0;

        boolean requeue =
                (flags & 0x02) != 0;

        eventSink.accept(
                new NackEvent(
                        connectionId,
                        channel,
                        deliveryTag,
                        multiple,
                        requeue
                )
        );
    }

    private void handleNonMethodFrame(
            String direction,
            int frameType,
            int channel,
            int payloadSize
    ) {
        int payloadOffset =
                FRAME_HEADER_SIZE;

        if (frameType == 2) {
            contentTracker.onHeader(
                    channel,
                    pending,
                    payloadOffset,
                    payloadSize
            );
        }

        if (frameType == 3) {
            contentTracker.onBody(
                    channel,
                    pending,
                    payloadOffset,
                    payloadSize
            );
        }

        logger.info(
                "{} | conn={} | {} | channel={} | payload={} bytes",
                direction,
                connectionId,
                frameTypeName(frameType),
                channel,
                payloadSize
        );
    }

    private int readPayloadSize() {
        return ((pending[3] & 0xFF) << 24)
                | ((pending[4] & 0xFF) << 16)
                | ((pending[5] & 0xFF) << 8)
                | (pending[6] & 0xFF);
    }

    private void discardFrame(
            int fullFrameSize
    ) {
        pending =
                Arrays.copyOfRange(
                        pending,
                        fullFrameSize,
                        pending.length
                );
    }

    private String frameTypeName(
            int type
    ) {
        return switch (type) {
            case 1 -> "METHOD";
            case 2 -> "HEADER";
            case 3 -> "BODY";
            case 8 -> "HEARTBEAT";
            default -> "UNKNOWN(" + type + ")";
        };
    }
}