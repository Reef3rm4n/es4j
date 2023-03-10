package io.vertx.eventx.exceptions;


import io.vertx.eventx.objects.EventxError;

public class CommandRejected extends EventxException {

  public CommandRejected(final EventxError eventxError) {
    super(eventxError);
  }

  public CommandRejected(final String cause, final String hint, final Integer errorCode) {
    super(cause, hint, errorCode);
  }


  public EventxError error() {
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
