package io.es4j.infrastructure.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileConfigurationCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileConfigurationCache.class);
  public static final Cache<String, JsonObject> FS_CONFIG_CACHE = Caffeine.newBuilder()
    .evictionListener((key, value, reason) -> LOGGER.info("File configuration evicted [" + reason + "]" + value))
    .removalListener((key, value, reason) -> LOGGER.info("File configuration removed [" + reason + "]" + value))
    .build();

  public static JsonObject get(String key) {
   return FS_CONFIG_CACHE.getIfPresent(key);
  }

  public static void put(String key, JsonObject value) {
    FS_CONFIG_CACHE.put(key, value);
  }

  public static void invalidate(String key) {
    FS_CONFIG_CACHE.invalidate(key);
  }

}
