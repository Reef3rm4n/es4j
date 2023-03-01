package io.vertx.eventx.config;


import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.shareddata.LocalMap;
import java.util.Set;

public class KubernetesConfiguration {
  private final Vertx vertx;
  public final String mapName;

  public KubernetesConfiguration(
    final String mapName,
    final Vertx vertx
  ) {
    this.mapName = mapName;
    this.vertx = vertx;
  }

  public JsonObject getRaw(String tenant) {
    return localMap().get(tenant);
  }

  public <T> T get(String tenant, Class<T> tClass) {
    return localMap().get(tenant).mapTo(tClass);
  }

  public Set<String> keySet() {
    return delegateMap().keySet();
  }

  private io.vertx.core.shareddata.LocalMap<String, JsonObject> delegateMap() {
    return vertx.getDelegate().sharedData().getLocalMap(mapName);
  }

  private LocalMap<String, JsonObject> localMap() {
    return vertx.sharedData().getLocalMap(mapName);
  }


}
