package io.es4j.http;


import com.google.auto.service.AutoService;
import io.es4j.infrastructure.models.ResetProjection;
import io.es4j.launcher.Es4jMain;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.Router;


import io.es4j.Aggregate;
import io.es4j.Bootstrap;
import io.es4j.infrastructure.models.ProjectionStream;
import io.es4j.infrastructure.proxy.AggregateEventBusPoxy;
import io.vertx.core.json.JsonArray;
import java.util.HashMap;
import java.util.Map;

import static io.es4j.core.CommandHandler.camelToKebab;


@AutoService(HttpRoute.class)
public class ProjectionRoute implements HttpRoute {

  private final Map<Class<? extends Aggregate>, AggregateEventBusPoxy<? extends Aggregate>> proxies = new HashMap<>();

  @Override
  public Uni<Void> start(Vertx vertx, JsonObject configuration) {
    Es4jMain.AGGREGATES.stream().map(Bootstrap::aggregateClass)
      .forEach(aggregateClass -> proxies.put(aggregateClass, new AggregateEventBusPoxy<>(vertx, aggregateClass)));
    return Uni.createFrom().voidItem();
  }

  @Override
  public void registerRoutes(Router router) {
    Es4jMain.AGGREGATES.stream().map(Bootstrap::aggregateClass).forEach(
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
