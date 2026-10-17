package dev.mockmq.tracking;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class MessageFingerprint {

    private MessageFingerprint() {}

    public static String of(String exchange, String routingKey, byte[] body) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            updateString(digest, exchange);
            updateString(digest, routingKey);

            digest.update(ByteBuffer.allocate(4).putInt(body.length).array());

            digest.update(body);

            byte[] hash = digest.digest();

            /*
             * We don't need the full SHA-256 in logs.
             * First 8 bytes -> 16 hex chars.
             */
            return HexFormat.of().formatHex(hash, 0, 8);

        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static void updateString(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

        digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());

        digest.update(bytes);
    }
}
