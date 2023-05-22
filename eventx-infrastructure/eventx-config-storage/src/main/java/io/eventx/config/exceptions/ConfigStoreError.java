package io.eventx.config.exceptions;

public record ConfigStoreError(
  String descrition,
  String cause,
  Integer internalCode
) {
}
