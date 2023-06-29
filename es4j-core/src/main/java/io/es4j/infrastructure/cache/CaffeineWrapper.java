package io.es4j.infrastructure.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.es4j.Aggregate;
import io.es4j.core.objects.AggregateConfiguration;
import io.es4j.core.objects.AggregateState;
import io.es4j.infrastructure.models.AggregatePlainKey;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class CaffeineWrapper {
  private CaffeineWrapper() {
  }

  private static final Logger logger = LoggerFactory.getLogger(CaffeineWrapper.class);
  public static Cache<AggregatePlainKey, Object> CAFFEINE;

  public static synchronized void setUp(AggregateConfiguration aggregateConfiguration) {
    if (Objects.isNull(CAFFEINE)) {
      CAFFEINE = Caffeine.newBuilder()
        .expireAfterAccess(aggregateConfiguration.aggregateCacheTtlInMinutes())
        .initialCapacity(500)
        .evictionListener((key, value, reason) -> logger.info("Aggregate evicted from cache {}", new JsonObject().put("reason", reason).put("key", key).encodePrettily()))
        .removalListener((key, value, reason) -> logger.info("Aggregate removed from cache {}", new JsonObject().put("reason", reason).put("key", key).encodePrettily()))
        .build();
    }
  }

  public static <T extends Aggregate> AggregateState<T> get(AggregatePlainKey k) {
    logger.debug("Fetching from cache {}", JsonObject.mapFrom(k).encodePrettily());
    final var valueObject = CAFFEINE.getIfPresent(k);
    if (valueObject != null) {
      logger.debug("Cache hit for {}", JsonObject.mapFrom(k).encodePrettily());
      return (AggregateState<T>) valueObject;
    }
    logger.debug("Cache miss for {}", JsonObject.mapFrom(k).encodePrettily());
    return null;
  }

  public static <T extends Aggregate> void put(AggregatePlainKey k, AggregateState<T> v) {
    logger.debug("Adding {}::{}", k, v);
    CAFFEINE.put(k, v);
  }

  public static <T extends Aggregate> void invalidate(Class<T> aggregateClass, AggregatePlainKey k) {
    logger.debug("Invalidating {}::{}", aggregateClass.getName(), k);
    CAFFEINE.invalidate(k);
  }
}
