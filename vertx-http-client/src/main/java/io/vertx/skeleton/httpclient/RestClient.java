package io.vertx.skeleton.httpclient;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.QueryOptions;
import io.vertx.skeleton.models.RequestMetadata;
import io.reactiverse.contextual.logging.ContextualData;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;

import java.time.Instant;
import java.util.List;

public abstract class RestClient {

  protected final String host;
  protected final Integer port;
  private final WebClient webClient;

  public abstract String basePath();

  protected HttpRequest<Buffer> get(String path, RequestMetadata requestMetadata) {
    return addHeaders(webClient.get(port, host, basePath() + (path == null ? "" : path)), requestMetadata);
  }

  protected HttpRequest<Buffer> post(String path, RequestMetadata requestMetadata) {
    return addHeaders(webClient.post(port, host, basePath() + (path == null ? "" : path)), requestMetadata);
  }

  protected HttpRequest<Buffer> post(RequestMetadata requestMetadata) {
    return addHeaders(webClient.post(port, host, basePath()), requestMetadata);
  }

  protected HttpRequest<Buffer> put(String path, RequestMetadata requestMetadata) {
    return addHeaders(webClient.put(port, host, basePath() + (path == null ? "" : path)), requestMetadata);
  }

  protected HttpRequest<Buffer> patch(String path, RequestMetadata requestMetadata) {
    return addHeaders(webClient.patch(port, host, basePath() + (path == null ? "" : path)), requestMetadata);
  }

  protected HttpRequest<Buffer> delete(String path, RequestMetadata requestMetadata) {
    return addHeaders(webClient.delete(port, host, basePath() + (path == null ? "" : path)), requestMetadata);
  }

  protected RestClient(String host, Integer port, WebClient webClient) {
    this.host = host;
    this.port = port;
    this.webClient = webClient;
  }

  private static final Logger logger = LoggerFactory.getLogger(RestClient.class);

  private static HttpRequest<Buffer> addHeaders(HttpRequest<Buffer> request, RequestMetadata requestMetadata) {
    if (requestMetadata != null) {
      ContextualData.put(RequestMetadata.X_TXT_ID, requestMetadata.txId());
      request
        .putHeader(RequestMetadata.CLIENT_ID, requestMetadata.clientId())
        .putHeader(RequestMetadata.CHANNEL_ID, requestMetadata.channelId())
        .putHeader(RequestMetadata.BRAND_ID_HEADER, String.valueOf(requestMetadata.tenant().brandId()))
        .putHeader(RequestMetadata.PARTNER_ID_HEADER, String.valueOf(requestMetadata.tenant().partnerId()))
        .putHeader(RequestMetadata.X_TXT_ID, requestMetadata.txId())
        .putHeader(RequestMetadata.TXT_DATE, Instant.now().toString())
        .putHeader(RequestMetadata.EXT_SYSTEM_ID, requestMetadata.externalSystemId())
        .putHeader(RequestMetadata.LONG_TERM_TOKEN, requestMetadata.longTermToken())
        .putHeader(RequestMetadata.PLAYER_ID, requestMetadata.userId());
    }
    return request;
  }

  public static void addFilterOptions(final QueryOptions options, final HttpRequest<Buffer> request) {
    if (Boolean.TRUE.equals(options.desc())) {
      request.addQueryParam("desc", "true");
    }
    addParam("creationDateFrom", options.creationDateFrom(), request);
    addParam("creationDateTo", options.creationDateTo(), request);
    addParam("lastUpdateFrom", options.lastUpdateFrom(), request);
    addParam("lastUpdateTo", options.lastUpdateTo(), request);
    addParam("pageNumber", options.pageNumber(), request);
    addParam("pageSize", options.pageSize(), request);
    addParam("orderBy", options.orderBy(), request);
  }

  public static void addParams(String name, List<?> queryParams, HttpRequest<Buffer> request) {
    if (queryParams != null && ! queryParams.isEmpty()) {
      queryParams.forEach(item -> {
          if (item instanceof Enum en) {
            request.addQueryParam(name, en.name());
          } else {
            request.addQueryParam(name, item.toString());
          }
        }
      );
    }
  }

  public static void addParam(String name, Object queryParams, HttpRequest<Buffer> request) {
    if (queryParams != null) {
      if (queryParams instanceof Enum en) {
        request.addQueryParam(name, en.name());
      } else {
        request.addQueryParam(name, queryParams.toString());
      }
    }
  }

  public Void checkNoContent(HttpResponse<Buffer> response) {
    checkNotFound(response);
    if (response.statusCode() != 204) {
      logger.error("Incorrect response code :" + response.statusCode());
      final var error = response.bodyAsJsonObject().mapTo(Error.class);
      throw new RestClientException(error);
    }
    return null;
  }

  public <T> T checkResponse(HttpResponse<Buffer> response, Class<T> tClass) {
    JsonObject t = response.bodyAsJsonObject();
    logger.debug("Retrieved object " + t.encodePrettily());
    checkNotFound(response);
    if (response.statusCode() == 200) {
      return t.mapTo(tClass);
    }
    final var error = response.bodyAsJsonObject().mapTo(Error.class);
    logger.error(error);
    throw new RestClientException(error);

  }

  public <T> T checkResponseCreated(HttpResponse<Buffer> response, Class<T> tClass) {
    JsonObject t = response.bodyAsJsonObject();
    logger.debug("Retrieved object " + t.encodePrettily());
    checkNotFound(response);
    if (response.statusCode() == 201) {
      return t.mapTo(tClass);
    }
    final var error = response.bodyAsJson(Error.class);
    throw new RestClientException(error);
  }

  public <T> T checkResponse(HttpResponse<Buffer> response, Class<T> tClass, String method) {
    JsonObject t = response.bodyAsJsonObject();
    logger.debug("Retrieved object " + t.encodePrettily());
    checkNotFound(response);
    if (response.statusCode() == 200) {
      return t.mapTo(tClass);
    }
    final var error = response.bodyAsJson(Error.class);
    logger.error("Error handling " + method);
    throw new RestClientException(error);
  }

  public <T> List<T> checkResponses(HttpResponse<Buffer> response, Class<T> tClass) {
    checkNotFound(response);
    if (response.statusCode() == 200) {
      return response.bodyAsJsonArray().stream()
        .map(json -> JsonObject.mapFrom(json).mapTo(tClass))
        .toList();
    }
    final var error = response.bodyAsJsonObject().mapTo(Error.class);
    logger.error(error);
    throw new RestClientException(error);
  }

  public Void checkCreated(final HttpResponse<Buffer> response) {
    checkNotFound(response);
    if (response.statusCode() != 201) {
      logger.debug("Wrong response code, was expecting 201 but got " + response.statusCode());
      final var error = response.bodyAsJson(Error.class);
      throw new RestClientException(error);
    }
    return null;
  }

  public Uni<Void> checkCreatedOrAccepted(final HttpResponse<Buffer> response) {
    checkNotFound(response);
    if (response.statusCode() == 201 || response.statusCode() == 202) {
      return Uni.createFrom().voidItem();
    } else if (response.statusCode() == 408 || response.statusCode() == 504) {
      logger.error(response.bodyAsString());
      final var error = response.bodyAsJsonObject().mapTo(Error.class);
      logger.error(error);
      throw new RestClientException(error);
    }
    final var error = response.bodyAsJsonObject().mapTo(Error.class);
    logger.error(error);
    throw new RestClientException(error);
  }

  private void checkNotFound(HttpResponse<Buffer> response) {
    if (response.statusCode() == 404) {
      logger.error(response.bodyAsString());
      final var error = response.bodyAsJsonObject().mapTo(Error.class);
      logger.error(error);
      throw new RestClientException(error);
    }
  }

}
