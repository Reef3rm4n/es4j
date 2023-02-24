package io.vertx.skeleton.evs.actors;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.Router;

import io.vertx.mutiny.ext.web.RoutingContext;
import io.vertx.skeleton.evs.Entity;
import io.vertx.skeleton.evs.objects.Command;
import io.vertx.skeleton.evs.objects.CommandWrapper;
import io.vertx.skeleton.evs.objects.CompositeCommandWrapper;
import io.vertx.skeleton.evs.objects.PublicCommand;
import io.vertx.skeleton.httprouter.Constants;
import io.vertx.skeleton.httprouter.VertxHttpRoute;

import java.util.List;
import java.util.Map;

public class RestChannelProxy implements VertxHttpRoute {

  private Class<? extends Entity> entityAggregateClass;

  protected static final Logger LOGGER = LoggerFactory.getLogger(RestChannelProxy.class);

  private ChannelProxy<? extends Entity> entityAggregateProxy;
  private Map<String, String> commandClassMap;

  public RestChannelProxy(Vertx vertx) {
    this.entityAggregateProxy = new ChannelProxy<>(vertx, entityAggregateClass);
  }

  @Override
  public void registerRoutes(Router router) {
    router.post("/" + entityAggregateClass.getSimpleName().toLowerCase() + "/command/composite/:entityId").consumes(Constants.APPLICATION_JSON).produces(Constants.APPLICATION_JSON)
      .handler(routingContext -> {
          final var entityId = routingContext.pathParam("entityId");
          final var metadata = extractHeaders(routingContext);
          final var publicCommands = unpackCommands(routingContext);
          final var commands = mapToCommand(publicCommands);
          entityAggregateProxy.handleCompositeCommand(new CompositeCommandWrapper(entityId, commands, metadata))
            .subscribe()
            .with(
              response -> ok(routingContext, response),
              routingContext::fail
            );
        }
      );

    router.post("/" + entityAggregateClass.getSimpleName().toLowerCase() + "/command/:entityId").consumes(Constants.APPLICATION_JSON).produces(Constants.APPLICATION_JSON)
      .handler(routingContext -> {
          final var entityId = routingContext.pathParam("entityId");
          final var metadata = extractHeaders(routingContext);
          final var command = routingContext.body().asJsonObject().mapTo(PublicCommand.class);
          final var commandClass = commandClass(command);
          final var compositeCommand = new CommandWrapper(entityId, new Command(commandClass, JsonObject.mapFrom(command.command())), metadata);
          entityAggregateProxy.forwardCommand(compositeCommand)
            .subscribe()
            .with(
              response -> ok(routingContext, response),
              routingContext::fail
            );
        }
      );

    router.get("/" + entityAggregateClass.getSimpleName().toLowerCase() + "/:entityId").produces(Constants.APPLICATION_JSON)
      .handler(routingContext -> {
          final var entityId = routingContext.pathParam("entityId");
          final var metadata = extractHeaders(routingContext);
          entityAggregateProxy.load(entityId, metadata)
            .subscribe()
            .with(
              response -> ok(routingContext, response),
              routingContext::fail
            );
        }
      );
  }

  private static List<PublicCommand> unpackCommands(RoutingContext routingContext) {
    return routingContext.body().asJsonArray().stream().map(cmdObject -> JsonObject.mapFrom(cmdObject).mapTo(PublicCommand.class)).toList();
  }

  private List<Command> mapToCommand(List<PublicCommand> publicCommands) {
    return publicCommands.stream()
      .map(cmd -> new Command(cmd.commandType(), JsonObject.mapFrom(cmd)))
      .toList();
  }

  public RestChannelProxy setEntityAggregateClass(Class<? extends Entity> tClass) {
    this.entityAggregateClass = tClass;
    return this;
  }

  private String commandClass(PublicCommand command) {
    return commandClassMap.get(command.commandType());
  }


  public RestChannelProxy setCommandClassMap(Map<String, String> commandClassMap) {
    this.commandClassMap = commandClassMap;
    return this;
  }
}
