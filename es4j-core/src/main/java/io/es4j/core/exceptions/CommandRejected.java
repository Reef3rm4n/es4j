package io.es4j.core.exceptions;


import io.es4j.core.objects.Es4jError;

public class CommandRejected extends Es4jException {

  public CommandRejected(final Es4jError es4jError) {
    super(es4jError);
  }

  public CommandRejected(final String cause, final String hint, final Integer errorCode) {
    super(cause, hint, errorCode);
  }


  public Es4jError error() {
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
