package io.vertx.eventx.core.objects;

import io.vertx.eventx.Aggregate;

import java.util.StringJoiner;

import static io.vertx.eventx.core.CommandHandler.camelToKebab;


public class EventbusStateProjection {

  public static final String STATE_PROJECTION = "state-projection";

  public static String subscriptionAddress(Class<? extends Aggregate> aggregateClass, String tenantId) {
    return new StringJoiner("/")
      .add(STATE_PROJECTION)
      .add(camelToKebab(aggregateClass.getSimpleName()))
      .add(tenantId)
      .toString();

  }


}
