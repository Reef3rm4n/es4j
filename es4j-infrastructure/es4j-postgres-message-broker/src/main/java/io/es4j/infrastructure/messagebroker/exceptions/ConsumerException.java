package io.es4j.infrastructure.messagebroker.exceptions;


public class ConsumerException extends QueueException {

  public ConsumerException(QueueError eventxError) {
    super(eventxError);
  }

  public ConsumerException(Throwable throwable) {
    super(throwable);
  }

  public ConsumerException(QueueError eventxError, Throwable throwable) {
    super(eventxError, throwable);
  }

  public ConsumerException(String cause, String hint, Integer errorCode) {
    super(cause, hint, errorCode);
  }

  public ConsumerException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, hint, errorCode, throwable);
  }

}
