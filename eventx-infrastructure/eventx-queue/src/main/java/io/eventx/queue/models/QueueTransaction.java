package io.eventx.queue.models;

public record QueueTransaction(
  Object connection
) {


  public <T> T cast(Class<T> target) {
    return target.cast(connection);
  }

}
