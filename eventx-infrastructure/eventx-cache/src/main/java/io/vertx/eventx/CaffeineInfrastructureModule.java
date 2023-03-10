package io.vertx.eventx;

import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.vertx.eventx.objects.EventxModule;

public class CaffeineInfrastructureModule extends EventxModule {

  @Provides
  @Inject
  CaffeineAggregateCache aggregateCache() {
    return new CaffeineAggregateCache();
  }


}
