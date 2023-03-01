package io.vertx.eventx.queue.models;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventXException;

public class MessageProcessorException extends EventXException {

  public MessageProcessorException(EventXError eventxError) {
    super(eventxError);
  }

  public MessageProcessorException(Throwable throwable) {
    super(throwable);
  }

  public MessageProcessorException(EventXError eventxError, Throwable throwable) {
    super(eventxError, throwable);
  }

  public MessageProcessorException(String cause, String hint, Integer errorCode) {
    super(cause, hint, errorCode);
  }

  public MessageProcessorException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, hint, errorCode, throwable);
  }

}
