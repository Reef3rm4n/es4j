package io.es4j.infrastructure.pgbroker.models;

public record ConsumerTransaction(
  Object connection
) {

    public <T> T getDelegate(Class<T> connectionClass) {
        return connectionClass.cast(connection);
    }
}
