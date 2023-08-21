package io.es4j.infrastructure.pgbroker.exceptions;

public class MessageParsingException extends RuntimeException{
  public MessageParsingException() {
  }

  public MessageParsingException(String message) {
    super(message);
  }

  public MessageParsingException(String message, Throwable cause) {
    super(message, cause);
  }

  public MessageParsingException(Throwable cause) {
    super(cause);
  }

  public MessageParsingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
