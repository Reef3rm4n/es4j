package io.vertx.eventx.queue.exceptions;


import io.vertx.eventx.exceptions.EventxException;
import io.vertx.eventx.objects.EventxError;

public class MessageProcessorException extends EventxException {

  public MessageProcessorException(EventxError eventxError) {
    super(eventxError);
  }

  public MessageProcessorException(Throwable throwable) {
    super(throwable);
  }

  public MessageProcessorException(EventxError eventxError, Throwable throwable) {
    super(eventxError, throwable);
  }

  public MessageProcessorException(String cause, String hint, Integer errorCode) {
    super(cause, hint, errorCode);
  }

  public MessageProcessorException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, hint, errorCode, throwable);
  }

}
