package io.es4j.config.exceptions;

public record ConfigStoreError(
  String descrition,
  String cause,
  Integer internalCode
) {
}
