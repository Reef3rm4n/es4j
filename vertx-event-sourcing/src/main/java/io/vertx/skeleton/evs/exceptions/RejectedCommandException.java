package io.vertx.skeleton.evs.exceptions;


import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class RejectedCommandException extends VertxServiceException {

  public RejectedCommandException(final Error error) {
    super(error);
  }

  public RejectedCommandException(final String cause, final String hint, final Integer errorCode) {
    super(cause, hint, errorCode);
  }


  public Error error() {
    return super.error();
  }

  public static RejectedCommandException commandRejected(String hint) {
    return new RejectedCommandException("Command rejected", hint, 400);
  }

  public static RejectedCommandException rejectCommand(String hint) {
    return new RejectedCommandException("Command rejected", hint, 400);
  }

  public static RejectedCommandException notFound(String hint) {
    return new RejectedCommandException("Not Found", hint, 400);
  }
}
