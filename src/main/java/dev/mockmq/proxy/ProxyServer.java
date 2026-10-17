package dev.mockmq.proxy;

import dev.mockmq.amqp.AmqpFrameParser;
import dev.mockmq.tracking.MessageJourneyTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

public class ProxyServer {

    private static final int PROXY_PORT = 5673;
    private static final int RABBIT_PORT = 5672;
    private static final String RABBIT_HOST = "localhost";

    private static final Logger logger =
            LoggerFactory.getLogger(ProxyServer.class);

    private static final AtomicLong CONNECTION_IDS =
            new AtomicLong();

    private static final MessageJourneyTracker JOURNEY_TRACKER =
            new MessageJourneyTracker();

    static void main(String[] args) throws Exception {

        try (ServerSocket serverSocket =
                     new ServerSocket(PROXY_PORT)) {

            logger.info(
                    "MockMQ listening on {}",
                    PROXY_PORT
            );

            while (true) {
                Socket clientSocket =
                        serverSocket.accept();

                long connectionId =
                        CONNECTION_IDS.incrementAndGet();

                logger.info(
                        "Accepted client connection {}",
                        connectionId
                );

                Thread.ofPlatform().start(() ->
                        handleConnection(
                                connectionId,
                                clientSocket
                        )
                );
            }
        }
    }

    private static void handleConnection(
            long connectionId,
            Socket clientSocket
    ) {
        try (
                clientSocket;
                Socket rabbitSocket =
                        new Socket(
                                RABBIT_HOST,
                                RABBIT_PORT
                        )
        ) {
            AmqpFrameParser clientToRabbitParser =
                    new AmqpFrameParser(
                            connectionId,
                            true,
                            JOURNEY_TRACKER
                    );

            AmqpFrameParser rabbitToClientParser =
                    new AmqpFrameParser(
                            connectionId,
                            false,
                            JOURNEY_TRACKER
                    );

            Thread clientToRabbit = new Thread(() ->
                    pipe(
                            clientSocket,
                            rabbitSocket,
                            "CLIENT -> RABBIT",
                            clientToRabbitParser
                    )
            );

            Thread rabbitToClient = new Thread(() ->
                    pipe(
                            rabbitSocket,
                            clientSocket,
                            "RABBIT -> CLIENT",
                            rabbitToClientParser
                    )
            );

            clientToRabbit.start();
            rabbitToClient.start();

            clientToRabbit.join();
            rabbitToClient.join();

        } catch (Exception e) {
            logger.error(
                    "Connection {} handling failed",
                    connectionId,
                    e
            );
        }
    }

    private static void pipe(
            Socket source,
            Socket target,
            String direction,
            AmqpFrameParser parser
    ) {
        try {
            InputStream in =
                    source.getInputStream();

            OutputStream out =
                    target.getOutputStream();

            byte[] buffer =
                    new byte[8192];

            int bytesRead;

            while ((bytesRead = in.read(buffer)) != -1) {

                parser.accept(
                        buffer,
                        bytesRead,
                        direction
                );

                out.write(
                        buffer,
                        0,
                        bytesRead
                );

                out.flush();
            }

        } catch (IOException e) {
            logger.info(
                    "{} pipe closed: {}",
                    direction,
                    e.getMessage()
            );
        }
    }
}