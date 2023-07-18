package io.es4j.infrastructure.messagebroker.exceptions;



public class MessageException extends QueueException {

  public MessageException(QueueError eventxError) {
    super(eventxError);
  }

}
