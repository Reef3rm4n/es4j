package io.eventx.http;


import com.google.auto.service.AutoService;
import io.eventx.infrastructure.models.ResetProjection;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.Router;


import io.eventx.Aggregate;
import io.eventx.Bootstrap;
import io.eventx.infrastructure.models.ProjectionStream;
import io.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.eventx.launcher.EventxMain;
import io.vertx.core.json.JsonArray;
import java.util.HashMap;
import java.util.Map;

import static io.eventx.core.CommandHandler.camelToKebab;


@AutoService(HttpRoute.class)
public class ProjectionRoute implements HttpRoute {

  private final Map<Class<? extends Aggregate>, AggregateEventBusPoxy<? extends Aggregate>> proxies = new HashMap<>();

  @Override
  public Uni<Void> start(Vertx vertx, JsonObject configuration) {
    EventxMain.AGGREGATES.stream().map(Bootstrap::aggregateClass)
      .forEach(aggregateClass -> proxies.put(aggregateClass, new AggregateEventBusPoxy<>(vertx, aggregateClass)));
    return Uni.createFrom().voidItem();
  }

  @Override
  public void registerRoutes(Router router) {
    EventxMain.AGGREGATES.stream().map(Bootstrap::aggregateClass).forEach(
      aClass -> {
        router.post("/%s/projection/next".formatted(camelToKebab(aClass.getSimpleName())))
          .consumes(Constants.APPLICATION_JSON)
          .produces(Constants.APPLICATION_JSON)
          .handler(
            routingContext -> proxies.get(aClass).projectionNext(routingContext.body().asJsonObject().mapTo(ProjectionStream.class))
              .subscribe()
              .with(
                events -> okWithArrayBody(routingContext, new JsonArray(events)),
                throwable -> respondWithUnmanagedError(routingContext, throwable)
              )
          );
        router.post("/%s/projection/reset".formatted(camelToKebab(aClass.getSimpleName())))
          .consumes(Constants.APPLICATION_JSON)
          .produces(Constants.APPLICATION_JSON)
          .handler(
            routingContext -> {
              final var json = routingContext.body().asJsonObject().mapTo(ResetProjection.class);
              proxies.get(aClass).resetProjection(json)
                .subscribe()
                .with(
                  events -> noContent(routingContext),
                  throwable -> respondWithUnmanagedError(routingContext, throwable)
                );
            }
          );
      }
    );
  }
}
