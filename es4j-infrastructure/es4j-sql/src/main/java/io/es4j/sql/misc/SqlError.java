package io.es4j.sql.misc;


public record SqlError(
  String errorMessage,
  String severity,
  String code,
  String detail
) {

}
