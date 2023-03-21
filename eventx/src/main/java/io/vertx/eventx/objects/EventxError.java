package io.vertx.eventx.objects;


public record EventxError(
  ErrorSource errorSource,
  String source,
  String cause,
  String hint,
  String internalCode,

  Integer externalErrorCode
) {

  public EventxError(String cause, String hint, Integer internalErrorCode) {
    this(ErrorSource.UNKNOWN, null, cause, hint, String.valueOf(internalErrorCode), 500);
  }
}
