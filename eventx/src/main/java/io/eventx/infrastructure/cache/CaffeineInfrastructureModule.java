package io.eventx.infrastructure.cache;

import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.eventx.core.objects.EventxModule;
import io.eventx.infrastructure.AggregateCache;

public class CaffeineInfrastructureModule extends EventxModule {

  @Provides
  @Inject
  AggregateCache aggregateCache() {
    return new CaffeineAggregateCache();
  }


}
