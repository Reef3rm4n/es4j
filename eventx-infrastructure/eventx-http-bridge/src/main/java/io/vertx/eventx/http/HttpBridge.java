package io.vertx.eventx.http;


import io.smallrye.mutiny.Uni;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;
import io.vertx.eventx.core.projections.EventbusEventStream;
import io.vertx.eventx.core.objects.EventbusStateProjection;
import io.vertx.eventx.infrastructure.bus.AggregateBus;
import io.vertx.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.vertx.eventx.launcher.EventxMain;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSBridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.auth.authentication.AuthenticationProvider;
import io.vertx.mutiny.ext.auth.jwt.JWTAuth;
import io.vertx.mutiny.ext.web.handler.sockjs.SockJSHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.eventx.core.exceptions.EventxException;
import io.vertx.eventx.infrastructure.Bridge;
import io.vertx.eventx.core.objects.EventxError;
import io.vertx.eventx.core.objects.PublicQueryOptions;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.micrometer.PrometheusScrapingHandler;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.http.HttpServer;
import io.vertx.mutiny.core.http.HttpServerRequest;
import io.vertx.mutiny.ext.healthchecks.HealthCheckHandler;
import io.vertx.mutiny.ext.web.Router;
import io.vertx.mutiny.ext.web.RoutingContext;
import io.vertx.mutiny.ext.web.handler.BodyHandler;
import io.vertx.mutiny.ext.web.handler.LoggerHandler;

import java.time.Instant;
import java.util.*;

import static io.vertx.eventx.core.CommandHandler.camelToKebab;


public class HttpBridge implements Bridge {

  protected static final Logger LOGGER = LoggerFactory.getLogger(HttpBridge.class);
  private final Handler<io.vertx.ext.web.RoutingContext> prometheusScrapingHandler = PrometheusScrapingHandler.create();
  public static final int HTTP_PORT = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));
  private final List<HttpRoute> routes;
  private final List<HealthCheck> healthChecks;
  private HttpServer httpServer;
  private Vertx vertx;
  private final CommandAuth commandAuth;

  private final Map<Class<? extends Aggregate>, AggregateEventBusPoxy<? extends Aggregate>> proxies = new HashMap<>();

  public HttpBridge(
    final CommandAuth commandAuth,
    final List<HttpRoute> routes,
    final List<HealthCheck> healthChecks
  ) {
    this.commandAuth = commandAuth;
    this.routes = routes;
    this.healthChecks = healthChecks;
  }

  @Override
  public Uni<Void> start(Vertx vertx, JsonObject configuration, List<Class<? extends Aggregate>> aggregateClasses) {
    this.vertx = vertx;
    this.httpServer = httpServer();
    final var router = Router.router(vertx);
    this.healthChecks(router);
    this.metrics(router);
    router.route().failureHandler(this::failureHandler);
    router.route().handler(LoggerHandler.create(LoggerFormat.SHORT));
    openApiRoute(router);
    router.route().handler(BodyHandler.create());
    startProxies(vertx);
    aggregateRoutes(router);
    aggregateWebSocket(router);
    return httpServer.requestHandler(router)
      .invalidRequestHandler(this::handleInvalidRequest)
      .exceptionHandler(throwable -> LOGGER.error("HTTP Server dropped exception", throwable))
      .listen(HTTP_PORT)
      .invoke(httpServer1 -> LOGGER.info("HTTP Server listening on port {}", httpServer1.actualPort()))
      .replaceWithVoid();
  }

  private void startProxies(Vertx vertx) {
    EventxMain.AGGREGATE_CLASSES.forEach(aggregateClass -> proxies.put(aggregateClass, new AggregateEventBusPoxy<>(vertx, aggregateClass)));
  }

  private void aggregateWebSocket(Router router) {
    final var options = new SockJSHandlerOptions().setRegisterWriteHandler(true);
    final var bridgeOptions = new SockJSBridgeOptions();
    EventxMain.AGGREGATE_CLASSES.forEach(
      aClass -> bridgeOptions
        .addInboundPermitted(permission(AggregateBus.COMMAND_BRIDGE, aClass))
        .addOutboundPermitted(permission(EventbusStateProjection.STATE_PROJECTION, aClass))
        .addOutboundPermitted(permission(EventbusEventStream.EVENT_PROJECTION, aClass))
    );
    final var subRouter = SockJSHandler.create(vertx, options).bridge(
      bridgeOptions,
      bridgeEvent -> {
        LOGGER.info("Bridge event {}::{}", bridgeEvent.type(), bridgeEvent.getRawMessage());
        bridgeEvent.tryComplete(true);
      }
    );
    router.route("/eventbus/*").subRouter(subRouter);
  }

  private static PermittedOptions permission(String permissionType, Class<? extends Aggregate> aClass) {
    return new PermittedOptions().setAddressRegex("^" + permissionType + "\\/" + camelToKebab(aClass.getSimpleName()) + "\\/.*");
  }

  private void aggregateRoutes(Router router) {
    EventxMain.AGGREGATE_COMMANDS.forEach((key, value) -> value.forEach(commandClass -> router.post(parsePath(key, commandClass))
        .consumes(Constants.APPLICATION_JSON)
        .produces(Constants.APPLICATION_JSON)
        .handler(routingContext -> {
            final var command = new JsonObject()
              .put("commandClass", commandClass.getName())
              .put("command", routingContext.body().asJsonObject());
            Optional.ofNullable(commandAuth).orElse((command1, headers) -> {
                  LOGGER.warn("Default CommandAuth for {} {} {}", commandClass.getSimpleName(), command1, headers.entries());
                  return Uni.createFrom().voidItem();
                }
              )
              .validateCommand(routingContext.body().asJsonObject().mapTo(commandClass), routingContext.request().headers())
              .flatMap(avoid -> proxies.get(key).forward(command))
              .subscribe()
              .with(
                state -> okJson(routingContext, state.toJson()),
                routingContext::fail
              );
          }
        )
      )
    );
  }

  public static String parsePath(Class<? extends Aggregate> aggregateClass, Class<? extends Command> commandClass) {
    return new StringJoiner("/", "/", "")
      .add(camelToKebab(aggregateClass.getSimpleName()))
      .add(camelToKebab(commandClass.getSimpleName()))
      .toString();
  }

  @Override
  public Uni<Void> close() {
    return httpServer.close();
  }

  private HttpServer httpServer() {
    return vertx.createHttpServer(new HttpServerOptions()
      .setTracingPolicy(TracingPolicy.ALWAYS)
      .setLogActivity(true)
      .setRegisterWebSocketWriteHandlers(true)
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
    final var baseHealthCheck = io.vertx.ext.healthchecks.HealthChecks.create(vertx.getDelegate());
    if (!healthChecks.isEmpty()) {
      healthChecks.forEach(
        healthCheck -> baseHealthCheck.register(
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
    router.get("/readiness")
      .handler(HealthCheckHandler.createWithHealthChecks(io.vertx.mutiny.ext.healthchecks.HealthChecks.newInstance(baseHealthCheck)))
      .failureHandler(this::failureHandler);
  }

  private void metrics(Router router) {
    router.route("/metrics")
      .handler(routingContext -> prometheusScrapingHandler.handle(routingContext.getDelegate()))
      .failureHandler(this::failureHandler);
  }


  private void failureHandler(RoutingContext routingContext) {
    if (routingContext.failure() instanceof final EventxException vertxServiceException) {
      respondWithServerManagedError(routingContext, vertxServiceException.error());
    } else {
      respondWithUnmanagedError(routingContext, routingContext.failure());
    }
  }


  public static void ok(RoutingContext routingContext, Object o) {
    routingContext.response().setStatusCode(200)
      .putHeader(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON)
      .sendAndForget(JsonObject.mapFrom(o).encode());
  }

  public static void okJson(RoutingContext routingContext, JsonObject o) {
    routingContext.response().setStatusCode(200)
      .putHeader(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON)
      .sendAndForget(Buffer.newInstance(o.toBuffer()));
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

  private static void respondWithServerManagedError(RoutingContext routingContext, EventxError eventxError) {
    routingContext.response()
      .setStatusCode(eventxError.externalErrorCode())
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
        JsonObject.mapFrom(new EventxError(throwable.getMessage(), cause, 500)).encode()
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
