package io.es4j.infrastructure.config;


import io.vertx.core.json.JsonObject;

public class FileBusinessRule {
  public static <T> T get(Class<T> tClass, String fileName) {
    return FileConfigurationCache.get(fileName).mapTo(tClass);
  }

  public static JsonObject get(String fileName) {
    return FileConfigurationCache.get(fileName);
  }

}
