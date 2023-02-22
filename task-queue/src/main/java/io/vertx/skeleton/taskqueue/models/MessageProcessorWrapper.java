package io.vertx.skeleton.taskqueue.models;

import io.vertx.skeleton.taskqueue.TaskProcessor;

import java.util.List;
import java.util.Map;

public record MessageProcessorWrapper<T> (
  String deploymentId,
  TaskProcessor<T> defaultProcessor,
  Map<List<String>, TaskProcessor<T>> customProcessors,
  Class<T> payloadClass
) {


  public TaskProcessor<T> resolveProcessor(String tenant) {
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
