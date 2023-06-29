package io.es4j.core.objects;

import io.es4j.Aggregate;
import io.es4j.core.CommandHandler;

import java.util.StringJoiner;


public class EventbusLiveStreams {

  public static final String STATE_STREAM = "state-stream";
  public static final String EVENT_STREAM = "event-stream";

  public static String stateLiveStream(Class<? extends Aggregate> aggregateClass, String aggregateId, String tenantId) {
    return new StringJoiner("/")
      .add(STATE_STREAM)
      .add(CommandHandler.camelToKebab(aggregateClass.getSimpleName()))
      .add(tenantId)
      .add(aggregateId)
      .toString();
  }

  public static String eventLiveStream(Class<? extends Aggregate> aggregateClass, String aggregateId, String tenantId) {
    return new StringJoiner("/")
      .add(EVENT_STREAM)
      .add(CommandHandler.camelToKebab(aggregateClass.getSimpleName()))
      .add(tenantId)
      .add(aggregateId)
      .toString();
  }


}
