package io.es4j.core.tasks;

import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.es4j.infrastructure.bus.AggregateBus;
import io.vertx.mutiny.core.Vertx;
import io.es4j.Aggregate;
import io.es4j.task.LockLevel;
import io.es4j.task.TimerTask;
import io.es4j.task.TimerTaskConfiguration;


import java.time.Duration;
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
      Duration.ofSeconds(5),
      Duration.ofMinutes(1),
      Duration.ofMinutes(1),
      Duration.ofMinutes(1),
      List.of()
    );
  }



}
