package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.ccp.ProcessEventConsumer;

public record EventConsumerWrapper<T,R>(
  ProcessEventConsumer<T,R> consumer,
  Class<R> resultClass
)  {

}
