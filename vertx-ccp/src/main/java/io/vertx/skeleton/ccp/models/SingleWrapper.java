package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.ccp.ProcessEventConsumer;
import io.vertx.skeleton.ccp.SingleProcessConsumer;
import io.vertx.skeleton.models.Tenant;
import io.vertx.core.impl.logging.Logger;
import java.util.List;
import java.util.Map;

public record SingleWrapper<T, R>(
  String deploymentId,
  SingleProcessConsumer<T,R> defaultProcessor,
  Map<List<Tenant>, SingleProcessConsumer<T,R>> customProcessors,
  List<ProcessEventConsumer<T, R>> consumers,
  Class<T> payloadClass,
  QueueConfiguration configuration,
  Logger logger
) {

}
