package io.vertx.eventx;

import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.vertx.eventx.objects.EventxModule;
import io.vertx.mutiny.core.Vertx;

public class EventbusProjectionsModule extends EventxModule {


  @Provides
  @Inject
  EventbusEventProjection eventbusEventProjection(Vertx vertx) {
    return new EventbusEventProjection(vertx);
  }

}
