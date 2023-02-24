package io.vertx.skeleton.evs.exceptions;


import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

public class CommandRejected extends VertxServiceException {

  public CommandRejected(final Error error) {
    super(error);
  }

  public CommandRejected(final String cause, final String hint, final Integer errorCode) {
    super(cause, hint, errorCode);
  }


  public Error error() {
    return super.error();
  }

  public static CommandRejected commandRejected(String hint) {
    return new CommandRejected("Command rejected", hint, 400);
  }

  public static CommandRejected rejectCommand(String hint) {
    return new CommandRejected("Command rejected", hint, 400);
  }

  public static CommandRejected notFound(String hint) {
    return new CommandRejected("Not Found", hint, 400);
  }
}
