package io.vertx.eventx;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.core.objects.AggregateState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.core.exceptions.CommandRejected;
import io.vertx.eventx.core.objects.EventxError;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;

import java.util.Objects;
import java.util.StringJoiner;

import static io.vertx.eventx.core.CommandHandler.camelToKebab;

public class AggregateHttpClient<T extends Aggregate> {

  private final WebClient webClient;
  private final Class<T> aggregateClass;

  protected AggregateHttpClient(
    final WebClient webClient,
    final Class<T> aggregateClass
  ) {
    this.webClient = webClient;
    this.aggregateClass = aggregateClass;
  }

  private static final Logger logger = LoggerFactory.getLogger(AggregateHttpClient.class);

  public <C extends Command> Uni<AggregateState<T>> forward(C command) {
    return webClient.post(parsePath(aggregateClass, command.getClass()))
      .sendJson(JsonObject.mapFrom(Objects.requireNonNull(command, "command must not be null")))
      .map(this::parseResponse);
  }

  private static String parsePath(Class<? extends Aggregate> aggregateClass, Class<? extends Command> commandClass) {
    return new StringJoiner("/", "/", "")
      .add(camelToKebab(aggregateClass.getSimpleName()))
      .add(camelToKebab(commandClass.getSimpleName()))
      .toString();
  }

  private AggregateState<T> parseResponse(HttpResponse<Buffer> response) {
//    logger.debug("Retrieved object {}", response.bodyAsJsonObject().encodePrettily());
    if (response.statusCode() == 200) {
      return AggregateState.fromJson(response.bodyAsJsonObject(), aggregateClass);
    }
    logger.debug("Command rejected {} {} {}", response.body().toString(), response.statusCode(), response.statusMessage());
    final var error = response.bodyAsJsonObject().mapTo(EventxError.class);
    throw new CommandRejected(error);
  }

}
