package io.vertx.eventx.infrastructure;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;


public interface Bridge {

  Uni<Void> start(Vertx vertx, JsonObject configuration);

  Uni<Void> close();



}
