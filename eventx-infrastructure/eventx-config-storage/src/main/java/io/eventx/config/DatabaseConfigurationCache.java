package io.eventx.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

public class DatabaseConfigurationCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseConfigurationCache.class);
  public static final Cache<String, JsonObject> DB_CONFIGURATIONS = Caffeine.newBuilder()
//    .executor(cmd -> context.runOnContext(cmd))
//    .recordStats()
    .initialCapacity(500)
    .evictionListener((key, value, reason) -> LOGGER.info("Configuration evicted from cache reason[" + reason + "]" + value))
    .removalListener((key, value, reason) -> LOGGER.info("Configuration removed from cache reason[" + reason + "]" + value))
    .build();

  public static JsonObject get(String key) {
    return DB_CONFIGURATIONS.getIfPresent(key);
  }

  public static void put(String key, JsonObject value) {
    DB_CONFIGURATIONS.put(key, value);
  }

  public static void invalidate(String parseKey) {
    DB_CONFIGURATIONS.invalidate(parseKey);
  }
}
