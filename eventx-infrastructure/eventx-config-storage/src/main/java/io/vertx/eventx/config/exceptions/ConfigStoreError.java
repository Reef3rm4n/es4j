package io.vertx.eventx.config.exceptions;

public record ConfigStoreError(
  String descrition,
  String cause,
  Integer internalCode
) {
}
