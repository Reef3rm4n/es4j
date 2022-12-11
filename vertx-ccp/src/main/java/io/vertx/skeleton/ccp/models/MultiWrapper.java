package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.ccp.MultiProcessConsumer;
import io.vertx.skeleton.models.Tenant;
import io.vertx.core.impl.logging.Logger;

import java.util.List;
import java.util.Map;

public record MultiWrapper<T>(
  String deploymentId,
  List<MultiProcessConsumer<T>> defaultProcessors,
  Map<List<Tenant>, MultiProcessConsumer<T>> customProcessors,
  Class<T> payloadClass,
  QueueConfiguration configuration,
  Logger logger
) {


}
