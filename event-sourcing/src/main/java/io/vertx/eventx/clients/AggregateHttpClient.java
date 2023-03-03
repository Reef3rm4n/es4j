package io.vertx.eventx.clients;

import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.exceptions.RestChannelError;
import io.vertx.eventx.infrastructure.pg.models.AggregateRecordKey;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;
import io.vertx.eventx.common.CommandHeaders;

public class AggregateHttpClient<T extends Aggregate> {

  protected final String host;
  protected final Integer port;
  private final WebClient webClient;
  private final String basePath;
  private final Class<T> aggregateClass;

  protected AggregateHttpClient(
    String host,
    Integer port,
    WebClient webClient,
    String basePath,
    Class<T> aggregateClass
  ) {
    this.host = host;
    this.port = port;
    this.webClient = webClient;
    this.basePath = basePath;
    this.aggregateClass = aggregateClass;
  }

  private static final Logger logger = LoggerFactory.getLogger(AggregateHttpClient.class);


  public Uni<T> load(String entityId, CommandHeaders commandHeaders) {
    return get("/" + aggregateClass.getSimpleName().toLowerCase() + "/load", commandHeaders)
      .sendJson(new AggregateRecordKey(entityId, commandHeaders.tenantId()))
      .map(this::parseResponse);
  }

  public <C extends Command> Uni<T> forward(C command) {
    return post("/" + aggregateClass.getSimpleName().toLowerCase() + "/command", command)
      .sendJson(JsonObject.mapFrom(command))
      .map(this::parseResponse);
  }

  protected HttpRequest<Buffer> get(String path, CommandHeaders commandHeaders) {
    return addHeaders(webClient.get(port, host, basePath + (path == null ? "" : path)), commandHeaders);
  }

  protected <C extends Command> HttpRequest<Buffer> post(String path, C commandHeaders) {
    return addHeaders(webClient.post(port, host, basePath + (path == null ? "" : path)), commandHeaders);
  }


  private static <C extends Command> HttpRequest<Buffer> addHeaders(HttpRequest<Buffer> request, C commandHeaders) {
    if (commandHeaders != null) {
      ContextualData.put(CommandHeaders.COMMAND_ID, commandHeaders.headers().commandID());
      ContextualData.put(CommandHeaders.TENANT_ID, commandHeaders.headers().tenantId());
      request
        .putHeader(CommandHeaders.COMMAND_CLASS, commandHeaders.getClass().getName())
        .putHeader(CommandHeaders.COMMAND_ID, commandHeaders.headers().commandID())
        .putHeader(CommandHeaders.TENANT_ID, commandHeaders.headers().tenantId())
        .putHeader(CommandHeaders.TOKEN, commandHeaders.headers().token());
    }
    return request;
  }

  private static <C extends Command> HttpRequest<Buffer> addHeaders(HttpRequest<Buffer> request, CommandHeaders commandHeaders) {
    if (commandHeaders != null) {
      ContextualData.put(CommandHeaders.COMMAND_ID, commandHeaders.commandID());
      ContextualData.put(CommandHeaders.TENANT_ID, commandHeaders.tenantId());
      request
        .putHeader(CommandHeaders.COMMAND_CLASS, AggregateRecordKey.class.getName())
        .putHeader(CommandHeaders.COMMAND_ID, commandHeaders.commandID())
        .putHeader(CommandHeaders.TENANT_ID, commandHeaders.tenantId())
        .putHeader(CommandHeaders.TOKEN, commandHeaders.token());
    }
    return request;
  }

  public T parseResponse(HttpResponse<Buffer> response) {
    JsonObject t = response.bodyAsJsonObject();
    logger.debug("Retrieved object " + t.encodePrettily());
    checkNotFound(response);
    if (response.statusCode() == 200) {
      return t.mapTo(aggregateClass);
    }
    final var error = response.bodyAsJsonObject().mapTo(EventXError.class);
    logger.error(error);
    throw new RestChannelError(error);
  }

  private void checkNotFound(HttpResponse<Buffer> response) {
    if (response.statusCode() == 404) {
      logger.error(response.bodyAsString());
      final var error = response.bodyAsJsonObject().mapTo(EventXError.class);
      logger.error(error);
      throw new RestChannelError(error);
    }
  }

}
