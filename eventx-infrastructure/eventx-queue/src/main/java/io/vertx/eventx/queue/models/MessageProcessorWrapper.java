package io.vertx.eventx.queue.models;

import io.vertx.eventx.queue.MessageProcessor;

import java.util.List;
import java.util.Map;

public record MessageProcessorWrapper<T> (
  String deploymentId,
  MessageProcessor<T> defaultProcessor,
  Map<List<String>, MessageProcessor<T>> customProcessors,
  Class<T> payloadClass
) {


  public MessageProcessor<T> resolveProcessor(String tenant) {
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
