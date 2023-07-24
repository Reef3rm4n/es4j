package io.es4j.infrastructure.pgbroker;

import io.es4j.http.HttpRoute;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.Router;

public class PgBrokerRoute implements HttpRoute {

  @Override
  public Uni<Void> start(Vertx vertx, JsonObject configuration) {
    return null;
  }

  @Override
  public void registerRoutes(Router router) {
    // todo implement end-points
    router.get("/postgres-message-broker/messages");
    router.get("/postgres-message-broker/dlq");
    router.get("/postgres-message-broker/tx");
    router.post("/postgres-message-broker/message");
    router.post("/postgres-message-broker/cancel");
    router.post("/postgres-message-broker/retry");
  }


}
