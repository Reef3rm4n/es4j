package io.vertx.skeleton.models;


public class GenericVerticleException extends RuntimeException {


  private final Error error;

  public GenericVerticleException(Error error) {
    this.error = error;
  }

  public Error error() {
    return error;
  }

  public static GenericVerticleException illegalState() {
    return new GenericVerticleException(new Error("Illegal state", "", 500));
  }


  public static GenericVerticleException notImplemented() {
    return new GenericVerticleException(new Error("Not Implemented", "", 500));
  }


  public static GenericVerticleException notImplemented(String hint) {
    return new GenericVerticleException(new Error("Not Implemented", hint, 500));
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
