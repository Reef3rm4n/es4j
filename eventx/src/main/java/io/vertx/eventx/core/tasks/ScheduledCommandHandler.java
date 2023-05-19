package io.vertx.eventx.core.tasks;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.core.objects.ScheduledCommand;
import io.vertx.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.vertx.eventx.launcher.EventxMain;
import io.vertx.eventx.queue.MessageProcessor;
import io.vertx.eventx.queue.models.QueueTransaction;
import io.vertx.mutiny.core.Vertx;

import java.util.List;

public class ScheduledCommandHandler implements MessageProcessor<ScheduledCommand> {
  private final List<AggregateEventBusPoxy> proxies;

  public ScheduledCommandHandler(Vertx vertx) {
    this.proxies = EventxMain.AGGREGATE_CLASSES.stream()
      .map(aggregateClass -> (AggregateEventBusPoxy) new AggregateEventBusPoxy<>(vertx, aggregateClass))
      .toList();
  }

  @Override
  public Uni<Void> process(ScheduledCommand payload, QueueTransaction queueTransaction) {
    final var jsonCommand = new JsonObject(payload.command());
    final var proxy = proxies.stream()
      .filter(p -> p.aggregateClass.getName().equals(jsonCommand.getString("aggregateClass")))
      .findFirst().orElseThrow();
    return proxy.forward(jsonCommand).replaceWithVoid();
  }
}
