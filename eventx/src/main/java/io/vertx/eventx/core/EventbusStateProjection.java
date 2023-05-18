package io.vertx.eventx.core;

import io.smallrye.mutiny.Uni;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.StateProjection;
import io.vertx.eventx.objects.AggregateState;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringJoiner;

import static io.vertx.eventx.core.AggregateVerticleLogic.camelToKebab;


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
