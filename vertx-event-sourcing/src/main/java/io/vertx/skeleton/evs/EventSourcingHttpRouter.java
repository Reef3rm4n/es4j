package io.vertx.skeleton.evs;

import io.vertx.skeleton.evs.handlers.AggregateHandlerProxy;
import io.vertx.skeleton.evs.objects.Command;
import io.vertx.skeleton.evs.objects.CommandWrapper;
import io.vertx.skeleton.evs.objects.CompositeCommandWrapper;
import io.vertx.skeleton.evs.objects.PublicCommand;
import io.vertx.skeleton.httprouter.Constants;
import io.vertx.skeleton.httprouter.RouterException;
import io.vertx.skeleton.models.*;
import io.vertx.skeleton.orm.RepositoryHandler;
import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.micrometer.PrometheusScrapingHandler;
import io.vertx.mutiny.core.http.HttpServer;
import io.vertx.mutiny.core.http.HttpServerRequest;
import io.vertx.mutiny.ext.healthchecks.HealthCheckHandler;
import io.vertx.mutiny.ext.web.Router;
import io.vertx.mutiny.ext.web.RoutingContext;
import io.vertx.mutiny.ext.web.handler.BodyHandler;
import io.vertx.mutiny.ext.web.handler.LoggerHandler;
import io.vertx.skeleton.models.Error;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class EventSourcingHttpRouter extends AbstractVerticle {
  protected static final Logger LOGGER = LoggerFactory.getLogger(EventSourcingHttpRouter.class);
  private final Class<? extends EntityAggregate> entityAggregateClass;
  protected RepositoryHandler repositoryHandler;
  private HttpServer httpServer;
  public static final int HTTP_PORT = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));

  private final List<? extends EventSourcingHttpRoute> routes;

  private final Map<String, String> commandClassMap = new HashMap<>();


  public EventSourcingHttpRouter(
    final List<? extends EventSourcingHttpRoute> routes,
    final Class<? extends EntityAggregate> entityAggregateClass
  ) {
    this.entityAggregateClass = entityAggregateClass;
    this.routes = routes;
  }

  @Override
  public Uni<Void> asyncStart() {
    LOGGER.info("Starting " + this.getClass().getSimpleName() + " " + this.deploymentID());
    this.repositoryHandler = RepositoryHandler.leasePool(config(), vertx);
    this.httpServer = vertx.createHttpServer(new HttpServerOptions()
      .setTracingPolicy(TracingPolicy.PROPAGATE)
    );
    final var router = Router.router(vertx);
    this.healthChecks(router);
    this.metrics(router);
    router.route().handler(LoggerHandler.create(LoggerFormat.SHORT));
    router.route().handler(BodyHandler.create());
    router.route().handler(routingContext -> {
        if (routingContext.request().getHeader(RequestMetadata.X_TXT_ID) != null) {
          ContextualData.put(RequestMetadata.X_TXT_ID, routingContext.request().getHeader(RequestMetadata.X_TXT_ID));
        }
        if (routingContext.request().getHeader(RequestMetadata.BRAND_ID_HEADER) != null) {
          ContextualData.put(RequestMetadata.BRAND_ID_HEADER, routingContext.request().getHeader(RequestMetadata.BRAND_ID_HEADER));
        }
        if (routingContext.request().getHeader(RequestMetadata.PARTNER_ID_HEADER) != null) {
          ContextualData.put(RequestMetadata.PARTNER_ID_HEADER, routingContext.request().getHeader(RequestMetadata.PARTNER_ID_HEADER));
        }
        routingContext.next();
      }
    );

    final var entityAggregateProxy = new AggregateHandlerProxy(vertx, entityAggregateClass);
    router.post("/command/composite/:entityId").consumes(Constants.APPLICATION_JSON).produces(Constants.APPLICATION_JSON)
      .handler(routingContext -> {
          final var entityId = routingContext.pathParam("entityId");
          final var metadata = extractMetadata(routingContext);
          final var publicCommands = unpackCommands(routingContext);
          final var commands = mapToCommand(publicCommands);
          entityAggregateProxy.handleCompositeCommand(new CompositeCommandWrapper(entityId, commands, metadata), routingContext);
        }
      );
    router.post("/command/:entityId").consumes(Constants.APPLICATION_JSON).produces(Constants.APPLICATION_JSON)
      .handler(routingContext -> {
          final var entityId = routingContext.pathParam("entityId");
          final var metadata = extractMetadata(routingContext);
          final var command = routingContext.body().asJsonObject().mapTo(PublicCommand.class);
          final var commandClass = commandClass(command);
          final var compositeCommand = new CommandWrapper(entityId, new Command(commandClass, JsonObject.mapFrom(command.command())), metadata);
          entityAggregateProxy.forwardCommand(compositeCommand, routingContext);
        }
      );
    router.get("/:entityId").produces(Constants.APPLICATION_JSON)
      .handler(routingContext -> {
          final var entityId = routingContext.pathParam("entityId");
          final var metadata = extractMetadata(routingContext);
          entityAggregateProxy.load(entityId, metadata, routingContext);
        }
      );
    routes.forEach(route -> {
        LOGGER.info("Registering event sourcing route -> " + route.getClass());
        route.registerRoutes(router, entityAggregateProxy);
      }
    );
    router.route().failureHandler(this::failureHandler);
    return httpServer.requestHandler(router)
      .invalidRequestHandler(this::handleInvalidRequest)
      .exceptionHandler(throwable -> LOGGER.error("HTTP Server error", throwable))
      .listen(HTTP_PORT)
      .invoke(httpServer1 -> LOGGER.info(this.getClass().getSimpleName() + " started in port -> " + httpServer1.actualPort()))
      .replaceWithVoid();
  }

  private static List<PublicCommand> unpackCommands(RoutingContext routingContext) {
    return routingContext.body().asJsonArray().stream().map(cmdObject -> JsonObject.mapFrom(cmdObject).mapTo(PublicCommand.class)).toList();
  }

  private List<Command> mapToCommand(List<PublicCommand> publicCommands) {
    return publicCommands.stream()
      .map(cmd -> new Command(cmd.commandType(), JsonObject.mapFrom(cmd)))
      .toList();
  }

  private String commandClass(PublicCommand command) {
    return commandClassMap.get(command.commandType());
  }

  private void handleInvalidRequest(final HttpServerRequest httpServerRequest) {
    final var json = new JsonObject()
      .put("method", httpServerRequest.method().name())
      .put("headers", httpServerRequest.headers().entries())
      .put("uri", httpServerRequest.absoluteURI());
    LOGGER.error("Invalid request -> " + json.encodePrettily());
  }

  private void healthChecks(Router router) {
    final var healthChecks = io.vertx.ext.healthchecks.HealthChecks.create(vertx.getDelegate());
    healthChecks.register(
      "database-health",
      promise -> repositoryHandler.sqlClient().preparedQuery("select datname from pg_database")
        .execute()
        .subscribe()
        .with(rowSet -> {
            LOGGER.info("Database in good health");
            promise.tryComplete();
          }
          , throwable -> {
            LOGGER.error("Database connection is bad shape", throwable);
            promise.tryFail(throwable.getMessage());
          }
        )
    );
    router.get("/readiness").handler(HealthCheckHandler.createWithHealthChecks(io.vertx.mutiny.ext.healthchecks.HealthChecks.newInstance(healthChecks)));
  }

  private void metrics(Router router) {
    router.route("/metrics").handler(routingContext -> PrometheusScrapingHandler.create().handle(routingContext.getDelegate()));
  }

  @Override
  public Uni<Void> asyncStop() {
    LOGGER.info("Stopping " + this.getClass().getSimpleName() + this.deploymentID() + " -> " + config().encodePrettily());
    return httpServer.close()
      .flatMap(aVoid -> repositoryHandler.shutDown());
  }

  String deploymentId = UUID.randomUUID().toString();

  @Override
  public String deploymentID() {
    return deploymentId;
  }

  private void failureHandler(RoutingContext routingContext) {
    if (routingContext.failure() instanceof final VertxServiceException vertxServiceException) {
      respondWithServerManagedError(routingContext, vertxServiceException.error());
    } else {
      final var error = failureHandlerHook(routingContext.failure());
      if (error != null) {
        respondWithServerManagedError(routingContext, error);
      } else {
        respondWithUnmanagedError(routingContext, routingContext.failure());
      }
    }
  }

  private Error failureHandlerHook(final Throwable throwable) {
    return null;
  }

  public static void ok(RoutingContext routingContext, Object o) {
    routingContext.response().setStatusCode(200)
      .putHeader(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON)
      .sendAndForget(JsonObject.mapFrom(o).encode());
  }

  public static void created(RoutingContext routingContext, Object o) {
    routingContext.response().setStatusCode(201)
      .putHeader(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON)
      .sendAndForget(JsonObject.mapFrom(o).encode());
  }

  public static void ok(RoutingContext routingContext, JsonObject o) {
    routingContext.response().setStatusCode(200)
      .putHeader(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON)
      .sendAndForget(o.encode());
  }

  public static void okWithArrayBody(RoutingContext routingContext, JsonArray jsonArray) {
    routingContext.response().setStatusCode(200)
      .putHeader(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON)
      .sendAndForget(jsonArray.encode());
  }

  public static void created(RoutingContext routingContext) {
    routingContext.response().setStatusCode(201).sendAndForget();
  }

  public static void accepted(RoutingContext routingContext) {
    routingContext.response().setStatusCode(202).sendAndForget();
  }

  public static void noContent(RoutingContext routingContext) {
    routingContext.response().setStatusCode(204).sendAndForget();
  }

  private static void respondWithServerManagedError(RoutingContext routingContext, Error error) {
    routingContext.response()
      .setStatusCode(error.errorCode())
      .putHeader(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON)
      .endAndForget(JsonObject.mapFrom(error).encode());
  }


  public static void respondWithUnmanagedError(RoutingContext routingContext, Throwable throwable) {
    final var cause = throwable.getCause() != null ? throwable.getCause().getMessage() : throwable.getMessage();
    routingContext.response()
      .setStatusCode(500)
      .putHeader(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON)
      .endAndForget(
        JsonObject.mapFrom(new Error(throwable.getMessage(), cause, 500)).encode()
      );
  }

  public static <T> T extractRequestObject(Class<T> clazz, RoutingContext routingContext) {
    try {
      final var json = routingContext.body().asJsonObject();
      LOGGER.debug("Request object extracted ->" + json.encodePrettily());
      return json.mapTo(clazz);
    } catch (Exception e) {
      throw new RouterException(e.getMessage(), "malformed request, please check that your json conforms with notifier models", 500);
    }
  }

  public static <T> List<T> extractRequestArray(Class<T> clazz, RoutingContext routingContext) {
    try {
      return routingContext.body().asJsonArray().stream().map(o -> JsonObject.mapFrom(o).mapTo(clazz)).toList();
    } catch (Exception e) {
      throw new RouterException(e.getMessage(), "malformed request, please check that your json conforms with notifier models", 500);
    }
  }

  public static RequestMetadata extractMetadata(RoutingContext routingContext) {
    return new RequestMetadata(
      routingContext.request().getHeader(RequestMetadata.CLIENT_ID),
      routingContext.request().getHeader(RequestMetadata.CHANNEL_ID),
      routingContext.request().getHeader(RequestMetadata.EXT_SYSTEM_ID),
      routingContext.request().getHeader(RequestMetadata.X_TXT_ID),
      routingContext.request().getHeader(RequestMetadata.TXT_DATE),
      Integer.parseInt(routingContext.request().getHeader(RequestMetadata.BRAND_ID_HEADER)),
      Integer.parseInt(routingContext.request().getHeader(RequestMetadata.PARTNER_ID_HEADER)),
      routingContext.request().getHeader(RequestMetadata.PLAYER_ID),
      routingContext.request().getHeader(RequestMetadata.LONG_TERM_TOKEN)
    );
  }

  public static RequestMetadata extractMetadataOrNull(RoutingContext routingContext) {
    if (routingContext.request().getHeader(RequestMetadata.PARTNER_ID_HEADER) != null && routingContext.request().getHeader(RequestMetadata.BRAND_ID_HEADER) != null) {
      return new RequestMetadata(
        routingContext.request().getHeader(RequestMetadata.CLIENT_ID),
        routingContext.request().getHeader(RequestMetadata.CHANNEL_ID),
        routingContext.request().getHeader(RequestMetadata.EXT_SYSTEM_ID),
        routingContext.request().getHeader(RequestMetadata.X_TXT_ID),
        routingContext.request().getHeader(RequestMetadata.TXT_DATE),
        Integer.parseInt(routingContext.request().getHeader(RequestMetadata.BRAND_ID_HEADER)),
        Integer.parseInt(routingContext.request().getHeader(RequestMetadata.PARTNER_ID_HEADER)),
        routingContext.request().getHeader(RequestMetadata.PLAYER_ID),
        routingContext.request().getHeader(RequestMetadata.LONG_TERM_TOKEN)
      );
    }
    return null;
  }

  public static PublicQueryOptions getQueryOptions(RoutingContext routingContext) {
    final var desc = routingContext.queryParam("desc").stream().findFirst();
    final var creationDateFrom = routingContext.queryParam("creationDateFrom").stream().findFirst().map(Instant::parse);
    final var creationDateTo = routingContext.queryParam("creationDateTo").stream().findFirst().map(Instant::parse);
    final var lastUpdateFrom = routingContext.queryParam("lastUpdateFrom").stream().findFirst().map(Instant::parse);
    final var lastUpdateTo = routingContext.queryParam("lastUpdateTo").stream().findFirst().map(Instant::parse);
    final var pageNumber = routingContext.queryParam("pageNumber").stream().findFirst().map(Integer::parseInt);
    final var pageSize = routingContext.queryParam("pageSize").stream().findFirst().map(Integer::parseInt);
    pageSize.ifPresent(
      pSize -> {
        if (pSize > 1000) {
          throw new RouterException("Page size can't be greater than 5000", "", 400);
        }
      }
    );
    return new PublicQueryOptions(
      Boolean.parseBoolean(desc.orElse("false")),
      creationDateFrom.orElse(null),
      creationDateTo.orElse(null),
      lastUpdateFrom.orElse(null),
      lastUpdateTo.orElse(null),
      pageNumber.orElse(0),
      pageSize.orElse(1000)
    );
  }

}
