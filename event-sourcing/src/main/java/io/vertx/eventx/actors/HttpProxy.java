package io.vertx.eventx.actors;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.eventx.http.Constants;
import io.vertx.eventx.http.HttpRoute;
import io.vertx.eventx.storage.pg.models.AggregateKey;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSBridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.Router;

import io.vertx.mutiny.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.eventx.Aggregate;

public class HttpProxy implements HttpRoute {

  private final Vertx vertx;
  private final Class<? extends Aggregate> aggregateClass;

  protected static final Logger LOGGER = LoggerFactory.getLogger(HttpProxy.class);
  private final ChannelProxy<? extends Aggregate> entityAggregateProxy;

  public HttpProxy(
    Vertx vertx,
    Class<? extends Aggregate> aggregateClass
  ) {
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
    this.entityAggregateProxy = new ChannelProxy<>(vertx,aggregateClass);
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
      .handler(routingContext -> entityAggregateProxy.wakeUp(routingContext.body().asJsonObject().mapTo(AggregateKey.class))
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
