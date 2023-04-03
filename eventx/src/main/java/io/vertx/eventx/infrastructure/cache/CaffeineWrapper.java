package io.vertx.eventx.infrastructure.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.models.AggregatePlainKey;
import io.vertx.eventx.objects.AggregateState;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class CaffeineWrapper {
  private CaffeineWrapper() {
  }

  private static final Logger logger = LoggerFactory.getLogger(CaffeineWrapper.class);
  public static final Cache<AggregatePlainKey, Object> CAFFEINE = Caffeine.newBuilder()
//    .executor(cmd -> context.runOnContext(cmd))
    .expireAfterAccess(Duration.of(20, ChronoUnit.MINUTES))
//    .recordStats()
    .initialCapacity(500)
    .evictionListener((key, value, reason) -> logger.info("Aggregate evicted from cache reason[" + reason + "]" + value))
    .removalListener((key, value, reason) -> logger.info("Aggregate removed from cache reason[" + reason + "]" + value))
    .build();

  public static <T extends Aggregate> AggregateState<T> get(Class<T> aggregateClass, AggregatePlainKey k) {
    final var valueObject = CAFFEINE.getIfPresent(k);
    if (valueObject != null) {
      return (AggregateState<T>) valueObject;
    }
    return null;
  }

  public static <T extends Aggregate> void put(Class<T> aggregateClass, AggregatePlainKey k, AggregateState<T> v) {
    CAFFEINE.put(k, v);
  }
}
