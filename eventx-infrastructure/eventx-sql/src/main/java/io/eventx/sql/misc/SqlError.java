package io.eventx.sql.misc;


public record SqlError(
  String errorMessage,
  String severity,
  String code,
  String detail
) {

}
