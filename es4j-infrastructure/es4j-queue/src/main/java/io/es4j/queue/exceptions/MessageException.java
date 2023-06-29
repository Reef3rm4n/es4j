package io.es4j.queue.exceptions;



public class MessageException extends QueueException {

  public MessageException(QueueError eventxError) {
    super(eventxError);
  }

}
