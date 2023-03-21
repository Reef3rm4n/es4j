package io.vertx.eventx.config;

public record ConfigStoreError(
  String descrition,
  String cause,
  Integer internalCode
) {
}
