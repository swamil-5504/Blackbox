package org.blackbox.message;

import java.util.Objects;

public record Message(
        String key,
        String value,
        long timestamp
) {

    public Message {
        Objects.requireNonNull(key, "Message key cannot be null");
        Objects.requireNonNull(value, "Message value cannot be null");
    }

    public static Message create(String key, String value) {
        return new Message(
                key,
                value,
                System.currentTimeMillis()
        );
    }
}