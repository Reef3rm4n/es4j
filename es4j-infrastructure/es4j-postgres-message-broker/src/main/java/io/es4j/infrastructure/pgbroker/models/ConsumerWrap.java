package io.es4j.infrastructure.pgbroker.models;



import io.es4j.infrastructure.pgbroker.QueueConsumer;

import java.util.List;
import java.util.Map;

public record ConsumerWrap<T> (
  String deploymentId,
  QueueConsumer<T> defaultProcessor,
  Map<List<String>, QueueConsumer<T>> customProcessors,
  Class<T> payloadClass
) {


  public QueueConsumer<T> resolveProcessor(String tenant) {
    return customProcessors.entrySet().stream()
      .filter(wrapper -> wrapper.getKey().stream().anyMatch(tenant::equals))
      .map(Map.Entry::getValue)
      .findFirst()
      .orElse(defaultProcessor);
  }

  public boolean doesMessageMatch(RawMessage rawMessage) {
    return payloadClass.getName().equals(rawMessage.payloadClass());
  }

}
