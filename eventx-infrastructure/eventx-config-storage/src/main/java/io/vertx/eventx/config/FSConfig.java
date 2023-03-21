package io.vertx.eventx.config;


import io.vertx.core.json.JsonObject;

import java.util.StringJoiner;

public record FSConfig<T extends ConfigurationEntry> (
  Class<T> tClass,
  String name
) {
  public JsonObject getRaw(String tenant) {
    return FsConfigCache.get(getKey(name, tenant));
  }
  public T get(String tenant) {
    return FsConfigCache.get(getKey(name, tenant)).mapTo(tClass);
  }

  public T get() {
    return FsConfigCache.get(getKey(name, "default")).mapTo(tClass);
  }

  public static String getKey(String name, String tenant) {
    return new StringJoiner("::").add(name).add(tenant).toString();
  }

}
