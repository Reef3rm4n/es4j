package io.vertx.eventx.queue.exceptions;



public class MessageException extends QueueException {

  public MessageException(QueueError eventxError) {
    super(eventxError);
  }

}
