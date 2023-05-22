package io.eventx.infrastructure;
import io.eventx.Aggregate;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.List;


public interface Bridge {

  Uni<Void> start(Vertx vertx, JsonObject configuration, List<Class<? extends Aggregate>> aggregateClasses);

  Uni<Void> close();



}
