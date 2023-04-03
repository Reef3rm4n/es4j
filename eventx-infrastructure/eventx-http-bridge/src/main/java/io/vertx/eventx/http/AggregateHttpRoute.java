package io.vertx.eventx.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.models.AggregatePlainKey;
import io.vertx.eventx.infrastructure.proxies.AggregateEventBusPoxy;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSBridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.Router;
import io.vertx.mutiny.ext.web.handler.sockjs.SockJSHandler;

public class AggregateHttpRoute implements HttpRoute {

  private final Vertx vertx;
  private final Class<? extends Aggregate> aggregateClass;

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateHttpRoute.class);
  private final AggregateEventBusPoxy<? extends Aggregate> entityAggregateProxy;

  public AggregateHttpRoute(
    final Vertx vertx,
    final Class<? extends Aggregate> aggregateClass
  ) {
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
    this.entityAggregateProxy = new AggregateEventBusPoxy<>(vertx,aggregateClass);
  }


  @Override
  public void registerRoutes(Router router) {
    router.post("/" + aggregateClass.getSimpleName().toLowerCase())
      .consumes(Constants.APPLICATION_JSON)
      .produces(Constants.APPLICATION_JSON)
      .handler(routingContext -> entityAggregateProxy.command(routingContext.body().asJsonObject())
        .subscribe()
        .with(
          state -> ok(routingContext, state),
          routingContext::fail
        )
      );
    router.get("/" + aggregateClass.getSimpleName().toLowerCase())
      .consumes(Constants.APPLICATION_JSON)
      .produces(Constants.APPLICATION_JSON)
      .handler(routingContext -> entityAggregateProxy.wakeUp(routingContext.body().asJsonObject().mapTo(AggregatePlainKey.class))
        .subscribe()
        .with(
          response -> ok(routingContext, response),
          routingContext::fail
        )
      );
    final var options = new SockJSHandlerOptions()
      .setRegisterWriteHandler(true)
      .setHeartbeatInterval(200);
    SockJSHandler sockJSHandler = SockJSHandler.create(vertx, options);
    SockJSBridgeOptions bridgeOptions = new SockJSBridgeOptions()
      .addOutboundPermitted(new PermittedOptions()
        .setAddressRegex("/" + aggregateClass.getSimpleName().toLowerCase() + "/*/*")
      )
      .addInboundPermitted(new PermittedOptions()
        .setAddressRegex("/" + aggregateClass.getSimpleName().toLowerCase() + "/bridge/command")
      );
    router.route("/eventbus/" + aggregateClass.getSimpleName().toLowerCase())
      .subRouter(sockJSHandler.bridge(bridgeOptions));

  }


}
