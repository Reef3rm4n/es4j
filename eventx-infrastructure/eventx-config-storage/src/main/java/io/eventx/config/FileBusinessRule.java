package io.eventx.config;


import io.vertx.core.json.JsonObject;

public record FileBusinessRule<T extends BusinessRule> (
  Class<T> tClass,
  String fileName
) {
  public JsonObject getRaw() {
    return FsConfigCache.get(fileName);
  }

  public T get() {
    return FsConfigCache.get(fileName).mapTo(tClass);
  }

}
