package io.vertx.eventx.common;


public record EventxError(
  ErrorSource errorSource,
  String source,
  String cause,
  String hint,
  Integer internalErrorCode
) {

  public EventxError(String cause, String hint, Integer internalErrorCode) {
    this(ErrorSource.UNKNOWN, null, cause, hint, internalErrorCode);
  }
}
