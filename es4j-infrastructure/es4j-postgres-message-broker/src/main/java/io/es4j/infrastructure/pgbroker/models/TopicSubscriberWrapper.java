package io.es4j.infrastructure.pgbroker.models;


import io.es4j.infrastructure.pgbroker.TopicSubscription;
import io.es4j.infrastructure.pgbroker.exceptions.MessageParsingException;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;

import java.util.regex.Pattern;

public record TopicSubscriberWrapper<T>(
  TopicSubscription<T> consumer,
  Class<T> messageClass,
  Pattern topicPattern
) {
  public boolean match(RawMessage rawMessage) {
    return consumer.address().equals(rawMessage.messageAddress()) || topicPattern.matcher(rawMessage.messageAddress()).matches();
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
