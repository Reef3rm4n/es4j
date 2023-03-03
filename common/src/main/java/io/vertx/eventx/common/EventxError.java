package io.vertx.eventx.common;

import io.vertx.core.json.JsonObject;

public record EventxError(
  ErrorSource errorSource,
  String source,
  String cause,
  String hint,
  Integer internalErrorCode
) {
}
