package io.vertx.eventx.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.eventx.infrastructure.models.AggregatePlainKey;

public class CaffeineKube {

  private static final Logger LOGGER = LoggerFactory.getLogger(CaffeineKube.class);
  public static final Cache<AggregatePlainKey, Object> KUBE_CONFIG_CACHE = Caffeine.newBuilder()
//    .executor(cmd -> context.runOnContext(cmd))
//    .recordStats()
    .initialCapacity(500)
    .evictionListener((key, value, reason) -> LOGGER.info("Kube config evicted from cache reason[" + reason + "]" + value))
    .removalListener((key, value, reason) -> LOGGER.info("Kube config removed from cache reason[" + reason + "]" + value))
    .build();


}
