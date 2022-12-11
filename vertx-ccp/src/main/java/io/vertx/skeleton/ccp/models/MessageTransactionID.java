package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.models.Tenant;

public record MessageTransactionID(
  String messageId,
  String processorClass,
  Tenant tenant
) {
}
