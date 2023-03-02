package io.vertx.eventx.http;


import io.activej.inject.Injector;

import io.activej.inject.module.ModuleBuilder;
import io.vertx.eventx.common.*;
import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.eventx.common.exceptions.EventXException;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.micrometer.PrometheusScrapingHandler;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.http.HttpServer;
import io.vertx.mutiny.core.http.HttpServerRequest;
import io.vertx.mutiny.ext.healthchecks.HealthCheckHandler;
import io.vertx.mutiny.ext.web.FileUpload;
import io.vertx.mutiny.ext.web.Router;
import io.vertx.mutiny.ext.web.RoutingContext;
import io.vertx.mutiny.ext.web.handler.BodyHandler;
import io.vertx.mutiny.ext.web.handler.LoggerHandler;
import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.CustomClassLoader;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class HttpRouter extends AbstractVerticle {
  protected static final Logger LOGGER = LoggerFactory.getLogger(HttpRouter.class);
  private final ModuleBuilder moduleBuilder;
  private HttpServer httpServer;
  public static final int HTTP_PORT = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));
  private final Handler<io.vertx.ext.web.RoutingContext> prometheusScrapingHandler = PrometheusScrapingHandler.create();
  private Injector injector;

  public HttpRouter(ModuleBuilder moduleBuilder) {
    this.moduleBuilder = moduleBuilder;
  }


  private Injector startInjector() {
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    moduleBuilder.bind(JsonObject.class).toInstance(config());
    return Injector.of(moduleBuilder.build());
  }

  @Override
  public Uni<Void> asyncStart() {
    LOGGER.info("Starting " + this.getClass().getSimpleName() + " " + this.deploymentID());
    this.injector = startInjector();
    this.httpServer = httpServer();
    final var router = Router.router(vertx);

    this.healthChecks(router);
    this.metrics(router);
    router.route().handler(LoggerHandler.create(LoggerFormat.SHORT));
    openApiRoute(router);
    router.route().handler(BodyHandler.create());
    router.route().handler(routingContext -> {
        ContextualData.put("verticle-type", HttpRouter.class.getSimpleName());
        if (routingContext.request().getHeader(CommandHeaders.COMMAND_ID) != null) {
          ContextualData.put(CommandHeaders.COMMAND_ID, routingContext.request().getHeader(CommandHeaders.COMMAND_ID));
        }
        routingContext.next();
      }
    );
    final var routes = CustomClassLoader.loadFromInjector(injector, HttpRoute.class);
    routes.forEach(route -> route.registerRoutes(router));
    router.route().failureHandler(this::failureHandler);
    return httpServer.requestHandler(router)
      .invalidRequestHandler(this::handleInvalidRequest)
      .exceptionHandler(throwable -> LOGGER.error("HTTP Server error", throwable))
      .listen(HTTP_PORT)
      .invoke(httpServer1 -> LOGGER.info(this.getClass().getSimpleName() + " started in port -> " + httpServer1.actualPort()))
      .replaceWithVoid();
  }

  private HttpServer httpServer() {
    return vertx.createHttpServer(new HttpServerOptions()
        .setTracingPolicy(TracingPolicy.PROPAGATE)
        .setLogActivity(false)
//      .setUseAlpn(true)
//      .setReusePort(true)
//      .setTcpCork(true)
//      .setTcpFastOpen(true)
//      .setTcpNoDelay(true)
//      .setTcpQuickAck(true)
    );
  }

  private void openApiRoute(Router router) {
    router.get("/openapi.json")
      .handler(routingContext -> vertx.fileSystem().readFile("openapi.json")
        .subscribe()
        .with(routingContext::endAndForget,
          throwable -> LOGGER.error("Unable to fetch openapi.json", throwable)
        )
      );
    router.get("/openapi.yaml")
      .handler(routingContext -> vertx.fileSystem().readFile("openapi.yaml")
        .subscribe()
        .with(routingContext::endAndForget,
          throwable -> LOGGER.error("Unable to fetch openapi.json", throwable)
        )
      );
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
    final var extraHealthChecks = CustomClassLoader.loadFromInjector(injector, HealthCheck.class);
    if (!extraHealthChecks.isEmpty()) {
      extraHealthChecks.forEach(
        healthCheck -> healthChecks.register(
          healthCheck.name(),
          promise -> healthCheck.checkHealth()
            .subscribe()
            .with(
              promise::tryComplete
              , throwable -> {
                LOGGER.error(healthCheck.name() + " health check failed", throwable);
                promise.tryComplete(Status.KO(new JsonObject().put("message", throwable.getMessage())));
              }
            )
        )
      );
    }
//    healthChecks.register(
//      "database-health",
//      promise -> repositoryHandler.sqlClient().query("select datname from pg_database")
//        .execute()
//        .subscribe()
//        .with(rowSet -> promise.tryComplete(Status.OK())
//          , throwable -> {
//            LOGGER.error("Database connection is bad shape", throwable);
//            promise.tryComplete(Status.KO(new JsonObject().put("message", throwable.getMessage())));
//          }
//        )
//    );
    router.get("/readiness").handler(HealthCheckHandler.createWithHealthChecks(io.vertx.mutiny.ext.healthchecks.HealthChecks.newInstance(healthChecks)))
      .failureHandler(this::failureHandler);
  }

  private void metrics(Router router) {
    router.route("/metrics").handler(routingContext -> prometheusScrapingHandler.handle(routingContext.getDelegate()))
      .failureHandler(this::failureHandler);
  }

  @Override
  public Uni<Void> asyncStop() {
    LOGGER.info("Stopping " + this.getClass().getSimpleName() + this.deploymentID());
    return httpServer.close();
  }

  String deploymentId = UUID.randomUUID().toString();

  @Override
  public String deploymentID() {
    return deploymentId;
  }

  private void failureHandler(RoutingContext routingContext) {
    routingContext.failure();
    if (routingContext.failure() instanceof final EventXException vertxServiceException) {
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

  private EventXError failureHandlerHook(final Throwable throwable) {
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

  private static void respondWithServerManagedError(RoutingContext routingContext, EventXError eventxError) {
    routingContext.response()
      .setStatusCode(eventxError.errorCode())
      .putHeader(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON)
      .endAndForget(JsonObject.mapFrom(eventxError).encode());
  }


  public static void respondWithUnmanagedError(RoutingContext routingContext, Throwable throwable) {
    final var cause = throwable.getCause() != null ? throwable.getCause().getMessage() : throwable.getMessage();
    LOGGER.error("Unhandled throwable", throwable);
    routingContext.response()
      .setStatusCode(500)
      .putHeader(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON)
      .endAndForget(
        JsonObject.mapFrom(new EventXError(throwable.getMessage(), cause, 500)).encode()
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
          throw new RouterException("Page size can't be greater than 1000", "", 400);
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
