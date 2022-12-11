package io.vertx.skeleton.config;


import io.vertx.skeleton.models.Tenant;
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

  public JsonObject getRaw(Tenant tenant) {
    return localMap().get(tenant);
  }

  public <T> T get(Tenant tenant, Class<T> tClass) {
    return localMap().get(tenant).mapTo(tClass);
  }

  public Set<Tenant> keySet() {
    return delegateMap().keySet();
  }

  private io.vertx.core.shareddata.LocalMap<Tenant, JsonObject> delegateMap() {
    return vertx.getDelegate().sharedData().getLocalMap(mapName);
  }

  private LocalMap<Tenant, JsonObject> localMap() {
    return vertx.sharedData().getLocalMap(mapName);
  }


}
