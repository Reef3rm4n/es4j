package io.vertx.skeleton.ccp.models;

public record EventConsumerWrapper<T,R>(
  QueueEventConsumer<T,R> consumer,
  Class<R> resultClass
)  {

}
