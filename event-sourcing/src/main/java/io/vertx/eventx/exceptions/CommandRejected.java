package io.vertx.eventx.exceptions;


import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class CommandRejected extends EventXException {

  public CommandRejected(final EventXError eventxError) {
    super(eventxError);
  }

  public CommandRejected(final String cause, final String hint, final Integer errorCode) {
    super(cause, hint, errorCode);
  }


  public EventXError error() {
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
