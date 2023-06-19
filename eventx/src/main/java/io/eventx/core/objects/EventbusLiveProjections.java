package io.eventx.core.objects;

import io.eventx.Aggregate;
import io.eventx.core.CommandHandler;

import java.util.StringJoiner;


public class EventbusLiveProjections {

  public static final String STATE_PROJECTION = "state-projection";
  public static final String EVENT_PROJECTION = "event-projection";

  public static String subscriptionAddress(Class<? extends Aggregate> aggregateClass, String tenantId) {
    return new StringJoiner("/")
      .add(STATE_PROJECTION)
      .add(CommandHandler.camelToKebab(aggregateClass.getSimpleName()))
      .add(tenantId)
      .toString();
  }

  public static String eventSubscriptionAddress(Class<? extends Aggregate> aggregateClass, String tenantId) {
    return new StringJoiner("/")
      .add(EVENT_PROJECTION)
      .add(CommandHandler.camelToKebab(aggregateClass.getSimpleName()))
      .add(tenantId)
      .toString();
  }


}
