package io.es4j.infrastructure.pgbroker.models;


import io.es4j.infrastructure.pgbroker.QueueConsumer;
import io.es4j.infrastructure.pgbroker.exceptions.MessageParsingException;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;

public record QueueConsumerWrapper<T>(
  QueueConsumer<T> consumer,
  Class<T> messageClass
) {
  public boolean isMatch(RawMessage rawMessage) {
    return consumer.address().equals(rawMessage.messageAddress());
  }

  public Uni<Void> consume(RawMessage rawMessage, ConsumerTransaction consumerTransaction) {
    return consumer.process(
      (messageClass.isAssignableFrom(JsonObject.class)) ?
        messageClass.cast(rawMessage.payload()) :
        parseMessage(rawMessage),
      consumerTransaction
    );
  }

  private T parseMessage(RawMessage rawMessage) {
    try {
      if (consumer.schemaVersion() > rawMessage.schemaVersion()) {
        return consumer.migrate(rawMessage.payload(),rawMessage.schemaVersion());
      }
      return rawMessage.payload().mapTo(messageClass);
    } catch (Exception e) {
      throw new MessageParsingException(e);
    }
  }

}
