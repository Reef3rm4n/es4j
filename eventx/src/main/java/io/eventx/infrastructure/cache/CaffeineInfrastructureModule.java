package io.eventx.infrastructure.cache;

import com.google.auto.service.AutoService;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.eventx.core.objects.EventxModule;
import io.eventx.infrastructure.AggregateCache;

@AutoService(EventxModule.class)
public class CaffeineInfrastructureModule extends EventxModule {

  @Provides
  @Inject
  AggregateCache aggregateCache() {
    return new CaffeineAggregateCache();
  }


}
