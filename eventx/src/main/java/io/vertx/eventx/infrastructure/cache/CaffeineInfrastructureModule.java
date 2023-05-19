package io.vertx.eventx.infrastructure.cache;

import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.vertx.eventx.infrastructure.AggregateCache;
import io.vertx.eventx.core.objects.EventxModule;

public class CaffeineInfrastructureModule extends EventxModule {

  @Provides
  @Inject
  AggregateCache aggregateCache() {
    return new CaffeineAggregateCache();
  }


}
