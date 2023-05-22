package io.eventx.core.objects;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
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
