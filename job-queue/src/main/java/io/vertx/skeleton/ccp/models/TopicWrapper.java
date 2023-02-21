package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.models.Tenant;
import io.vertx.core.impl.logging.Logger;

import java.util.List;
import java.util.Map;

public record TopicWrapper<T>(
  String deploymentId,
  List<TopicMessageProcessor<T>> defaultProcessors,
  Map<List<Tenant>, TopicMessageProcessor<T>> customProcessors,
  Class<T> payloadClass,
  QueueConfiguration configuration,
  Logger logger
) {


}
