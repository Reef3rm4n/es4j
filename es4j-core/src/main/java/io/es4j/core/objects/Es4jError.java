package io.es4j.core.objects;

import io.soabase.recordbuilder.core.RecordBuilder;

@RecordBuilder
public record Es4jError(
  ErrorSource errorSource,
  String source,
  String cause,
  String hint,
  String internalCode,

  Integer externalErrorCode
) {

  public Es4jError(String cause, String hint, Integer internalErrorCode) {
    this(ErrorSource.UNKNOWN, null, cause, hint, String.valueOf(internalErrorCode), 500);
  }
}
