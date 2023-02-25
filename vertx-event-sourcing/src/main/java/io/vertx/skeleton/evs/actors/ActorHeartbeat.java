package io.vertx.skeleton.evs.actors;

import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.evs.Entity;


import static io.vertx.skeleton.evs.actors.ChannelProxy.LOGGER;

public class ActorHeartbeat<T extends Entity> {

  private final long refreshTaskTimerId;
  private final Vertx vertx;


  public ActorHeartbeat(
    Vertx vertx,
    Class<T> entityClass,
    long delay
  ) {
    this.vertx = vertx;
    this.refreshTaskTimerId = vertx.setTimer(
      delay,
      d -> {
        LOGGER.info("Invoking broadcast of entity " + entityClass.getName());
        Channel.invokeBroadCast(entityClass, vertx);
      }
    );
  }


  private void stop() {
    vertx.cancelTimer(refreshTaskTimerId);
  }

}
