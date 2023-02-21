package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.ccp.QueueMessageProcessor;
import io.vertx.core.impl.logging.Logger;
import java.util.List;
import java.util.Map;

public record MessageProcessorWrapper<T> (
  String deploymentId,
  QueueMessageProcessor<T> defaultProcessor,
  Map<List<String>, QueueMessageProcessor<T>> customProcessors,
  Class<T> payloadClass,
  Logger logger
) {


  public QueueMessageProcessor<T> resolveProcessor(String tenant) {
    return customProcessors.entrySet().stream()
      .filter(wrapper -> wrapper.getKey().stream().anyMatch(tenant::equals))
      .map(Map.Entry::getValue)
      .findFirst()
      .orElse(defaultProcessor);
  }

  public boolean doesMessageMatch(MessageRecord message) {
    return payloadClass.getName().equals(message.payloadClass());
  }

}
