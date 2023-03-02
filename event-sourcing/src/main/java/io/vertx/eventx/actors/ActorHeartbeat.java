package io.vertx.eventx.actors;

import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.mutiny.core.Vertx;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.task.LockLevel;
import io.vertx.eventx.task.TimerTask;
import io.vertx.eventx.task.TimerTaskConfiguration;


import java.util.List;


public class ActorHeartbeat<T extends Aggregate> implements TimerTask {

  private final Vertx vertx;
  private final Class<T> aggregateCLass;
  protected static final Logger LOGGER = LoggerFactory.getLogger(ActorHeartbeat.class);

  public ActorHeartbeat(
    Vertx vertx,
    Class<T> entityClass
  ) {
    this.vertx = vertx;
    this.aggregateCLass = entityClass;
  }

  @Override
  public Uni<Void> performTask() {
    Channel.invokeActorsBroadcast(aggregateCLass, vertx);
    return Uni.createFrom().voidItem();
  }

  @Override
  public TimerTaskConfiguration configuration() {
    return new TimerTaskConfiguration(
      null,
      LockLevel.LOCAL,
      2500L,
      2500L,
      1L,
      1L,
      List.of()
    );
  }



}
