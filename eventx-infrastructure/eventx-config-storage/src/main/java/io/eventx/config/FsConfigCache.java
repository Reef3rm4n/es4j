package io.eventx.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

public class FsConfigCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(FsConfigCache.class);
  public static final Cache<String, JsonObject> FS_CONFIG_CACHE = Caffeine.newBuilder()
//    .executor(cmd -> context.runOnContext(cmd))
//    .recordStats()
    .initialCapacity(500)
    .evictionListener((key, value, reason) -> LOGGER.info("Configuration evicted from cache reason[" + reason + "]" + value))
    .removalListener((key, value, reason) -> LOGGER.info("Configuration removed from cache reason[" + reason + "]" + value))
    .build();

  public static JsonObject get(String key) {
   return FS_CONFIG_CACHE.getIfPresent(key);
  }

  public static void put(String key, JsonObject value) {
    FS_CONFIG_CACHE.put(key, value);
  }

  public static void delete(String key) {
    FS_CONFIG_CACHE.invalidate(key);
  }

}
