package io.vertx.eventx.infrastructure.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.models.AggregatePlainKey;
import io.vertx.eventx.core.objects.AggregateState;

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
    .evictionListener((key, value, reason) -> logger.debug("Aggregate evicted from cache {}", new JsonObject().put("reason", reason).put("aggregate", value).put("key", key).encodePrettily()))
    .removalListener((key, value, reason) -> logger.debug("Aggregate removed from cache {}", new JsonObject().put("reason", reason).put("aggregate", value).put("key", key).encodePrettily()))
    .build();

  public static <T extends Aggregate> AggregateState<T> get(Class<T> aggregateClass, AggregatePlainKey k) {
    logger.debug("Fetching from cache {}", JsonObject.mapFrom(k).encodePrettily());
    final var valueObject = CAFFEINE.getIfPresent(k);
    if (valueObject != null) {
      logger.debug("Cache hit for {}", JsonObject.mapFrom(k).encodePrettily());
      return (AggregateState<T>) valueObject;
    }
    logger.debug("Cache miss for {}", JsonObject.mapFrom(k).encodePrettily());
    return null;
  }

  public static <T extends Aggregate> void put(Class<T> aggregateClass, AggregatePlainKey k, AggregateState<T> v) {
    logger.debug("Adding {} to cache {}", aggregateClass.getName(), v.toJson().encodePrettily());
    CAFFEINE.put(k, v);
  }
}
