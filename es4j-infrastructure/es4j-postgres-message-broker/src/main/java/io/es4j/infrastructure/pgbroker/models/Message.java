package io.es4j.infrastructure.pgbroker.models;


import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Instant;
@RecordBuilder
public record Message<T>(
    String messageId,
    String tenant,
    String partitionKey,
    Instant scheduled,
    Instant expiration,
    Integer priority,
    T payload
) {

    public Message {
        if (priority != null && priority > 10) {
            throw new IllegalArgumentException("Max priority is 10");
        }
        if (messageId == null) {
            throw new IllegalArgumentException("Id must not be null");
        }
    }

    public static <P> Message<P> simple(String messageId, P payload) {
        return new Message<>(
            messageId,
            "default",
            null,
            null,
            null,
            null,
            payload
        );
    }
}
