package io.vertx.eventx.infrastructure.bus;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

@ProxyGen // Generate service proxies
@VertxGen
public interface EventBusBridge {


  Future<JsonObject> forwardCommand(JsonObject command);
  Future<JsonObject> aggregate(String aggregateId, String tenantId);



}
