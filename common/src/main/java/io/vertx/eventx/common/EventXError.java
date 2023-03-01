package io.vertx.eventx.common;



public record EventXError(
  String cause,
  String hint,
  Integer errorCode
){

}
