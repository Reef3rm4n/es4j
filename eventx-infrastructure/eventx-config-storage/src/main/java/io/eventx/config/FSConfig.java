package io.eventx.config;


import io.vertx.core.json.JsonObject;

public record FSConfig<T extends ConfigurationEntry> (
  Class<T> tClass,
  String name
) {
  public JsonObject getRaw() {
    return FsConfigCache.get(name);
  }

  public T get() {
    return FsConfigCache.get(name).mapTo(tClass);
  }

}
