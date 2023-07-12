package io.es4j.infrastructure.cache;


import com.google.auto.service.AutoService;
import io.es4j.Aggregate;
import io.es4j.core.objects.AggregateConfiguration;
import io.es4j.core.objects.AggregateState;
import io.es4j.infrastructure.models.AggregateKey;
import io.es4j.infrastructure.models.AggregatePlainKey;
import io.es4j.infrastructure.AggregateCache;
import io.smallrye.mutiny.Uni;


@AutoService(AggregateCache.class)
public class CaffeineAggregateCache implements AggregateCache {

  @Override
  public <T extends Aggregate> AggregateState<T> get(AggregateKey<T> aggregateKey) {
    return CaffeineWrapper.get(key(aggregateKey));
  }

  @Override
  public <T extends Aggregate> void put(AggregateKey<T> aggregateKey, AggregateState<T> aggregate) {
    CaffeineWrapper.put(key(aggregateKey), aggregate);
  }

  @Override
  public Uni<Void> setup(Class<? extends Aggregate> aggregateClass, AggregateConfiguration configuration) {
    CaffeineWrapper.setUp(configuration);
    return Uni.createFrom().voidItem();
  }


  public static <T extends Aggregate> AggregatePlainKey key(AggregateKey<T> aggregateKey) {
    return new AggregatePlainKey(aggregateKey.aggregateClass().getName(), aggregateKey.aggregateId(), aggregateKey.tenantId());
  }
}
