package io.vertx.eventx.core;

import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.eventx.infrastructure.bus.AggregateBus;
import io.vertx.mutiny.core.Vertx;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.task.LockLevel;
import io.vertx.eventx.task.TimerTask;
import io.vertx.eventx.task.TimerTaskConfiguration;


import java.util.List;


public class AggregateHeartbeat<T extends Aggregate> implements TimerTask {

  private final Vertx vertx;
  private final Class<T> aggregateCLass;
  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateHeartbeat.class);

  public AggregateHeartbeat(
    Vertx vertx,
    Class<T> entityClass
  ) {
    this.vertx = vertx;
    this.aggregateCLass = entityClass;
  }

  @Override
  public Uni<Void> performTask() {
    AggregateBus.invokeActorsBroadcast(aggregateCLass, vertx);
    return Uni.createFrom().voidItem();
  }

  @Override
  public TimerTaskConfiguration configuration() {
    return new TimerTaskConfiguration(
      LockLevel.NONE,
      2500L,
      2500L,
      1L,
      1L,
      List.of()
    );
  }



}
