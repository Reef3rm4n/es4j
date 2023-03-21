package io.vertx.eventx.infrastructure.cache;


import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.AggregateCache;
import io.vertx.eventx.infrastructure.models.AggregateKey;
import io.vertx.eventx.infrastructure.models.AggregatePlainKey;
import io.vertx.eventx.objects.AggregateState;
import org.jetbrains.annotations.NotNull;


public class CaffeineAggregateCache implements AggregateCache {

  @Override
  public <T extends Aggregate> AggregateState<T> get(AggregateKey<T> aggregateKey) {
    return CaffeineWrapper.get(aggregateKey.aggregateClass(), key(aggregateKey));
  }

  @NotNull
  private static <T extends Aggregate> AggregatePlainKey key(AggregateKey<T> aggregateKey) {
    return new AggregatePlainKey(aggregateKey.aggregateClass().getName(), aggregateKey.aggregateId(), aggregateKey.tenantId());
  }

  @Override
  public <T extends Aggregate> void put(AggregateKey<T> aggregateKey, AggregateState<T> aggregate) {
    CaffeineWrapper.put(aggregate.aggregateClass(), key(aggregateKey), aggregate);
  }
}
