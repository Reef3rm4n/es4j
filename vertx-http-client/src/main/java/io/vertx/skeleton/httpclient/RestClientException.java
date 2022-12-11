package io.vertx.skeleton.httpclient;

import io.vertx.skeleton.models.Error;

import java.util.StringJoiner;

public class RestClientException extends RuntimeException {

  private Error error;
  public RestClientException(Error error) {
    this.error = error;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RestClientException.class.getSimpleName() + "[", "]")
      .add("error=" + error)
      .toString();
  }
}
