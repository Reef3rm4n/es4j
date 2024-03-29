package io.es4j.infra.redis;

import com.google.auto.service.AutoService;
import io.es4j.Aggregate;
import io.es4j.Es4jDeployment;
import io.es4j.core.objects.ErrorSource;
import io.es4j.core.objects.Es4jErrorBuilder;
import io.es4j.infrastructure.EventStore;
import io.es4j.infrastructure.models.*;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.cpu.CpuCoreSensor;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.mutiny.redis.client.RedisAPI;
import io.vertx.mutiny.redis.client.Response;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.ResponseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static io.es4j.core.CommandHandler.camelToKebab;


@AutoService(EventStore.class)
public class RedisEventStore implements EventStore {
  public static final String TENANT_ID = "tenant-id";
  public static final String EVENT_CLASS = "event-class";
  public static final String COMMAND_ID = "command-id";
  public static final String EVENT = "event";
  public static final String TAGS = "tags";
  public static final String SCHEMA_VERSION = "schema-version";
  private static final Logger LOGGER = LoggerFactory.getLogger(RedisEventStore.class);
  private Redis redisClient;
  private RedisAPI redisApi;
  private Class<? extends Aggregate> aggregateClass;

  @Override
  public <T extends Aggregate> Uni<List<Event>> fetch(AggregateEventStream<T> aggregateEventStream) {
    return redisApi.xread(parseStreamArguments(aggregateEventStream))
      .map(response -> {
          LOGGER.debug("Reading from stream -> {}", response);
          RedisEventStore.checkResponse(response);
          return response.attributes().values().stream()
            .map(this::mapAttributes)
            .toList();
        }
      );
  }

  @Override
  public <T extends Aggregate> Uni<Void> stream(AggregateEventStream<T> aggregateEventStream, Consumer<Event> consumer) {
    return redisApi.xread(parseStreamArguments(aggregateEventStream))
      .map(RedisEventStore::checkResponse)
      .map(response -> {
          RedisEventStore.checkResponse(response);
          response.attributes().values().stream()
            .map(this::mapAttributes)
            .forEach(consumer);
          return response;
        }
      )
      .replaceWithVoid();
  }

  @Override
  public <T extends Aggregate> Uni<Void> append(AppendInstruction<T> appendInstruction) {
    final var streamName = aggregateStream(appendInstruction);
    return Multi.createFrom().iterable(appendInstruction.events())
      .onItem().transformToUniAndConcatenate(event -> redisApi.xadd(mapArgs(event, streamName)))
      .collect().asList()
      .replaceWithVoid();
  }

  @Override
  public <T extends Aggregate> Uni<Void> startStream(StartStream<T> appendInstruction) {
    return redisApi.sadd(List.of(aggregateClass.getSimpleName(), aggregateStream(appendInstruction)))
      .map(RedisEventStore::checkResponse)
      .replaceWithVoid();
  }


  private static Response checkResponse(Response response) {
    if (response.type() == ResponseType.ERROR) {
      throw new RedisEventStoreException(response.toBuffer().toString());
    }
    return response;
  }


  private List<String> mapArgs(Event event, String streamName) {
    return Arrays.asList(
      streamName, String.valueOf(event.eventVersion()),
      TENANT_ID, event.tenantId(),
      EVENT_CLASS, event.eventType(),
      COMMAND_ID, event.commandId(),
      EVENT, event.event().encode(),
      TENANT_ID, event.tenantId(),
      TAGS, new JsonArray(event.tags()).encode(),
      SCHEMA_VERSION, event.schemaVersion().toString()
    );
  }


  private <T extends Aggregate> String aggregateStream(StartStream<T> appendInstruction) {
    return aggregateClass.getSimpleName() + "-" + appendInstruction.aggregateId() + "-" + appendInstruction.tenantId();
  }

  private <T extends Aggregate> String aggregateStream(AppendInstruction<T> appendInstruction) {
    return aggregateClass.getSimpleName() + "-" + appendInstruction.aggregateId() + "-" + appendInstruction.tenantId();
  }

  private <T extends Aggregate> String aggregateStream(PruneEventStream<T> appendInstruction) {
    return aggregateClass.getSimpleName() + "-" + appendInstruction.aggregateId() + "-" + appendInstruction.tenantId();
  }

  private <T extends Aggregate> String aggregateStream(AggregateEventStream<T> aggregateEventStream) {
    return aggregateClass.getSimpleName() + "-" + aggregateEventStream.aggregateId() + "-" + aggregateEventStream.tenantId();
  }

  private <T extends Aggregate> List<String> parseStreamArguments(AggregateEventStream<T> aggregateEventStream) {
    return Arrays.asList(
      aggregateStream(aggregateEventStream), String.valueOf(aggregateEventStream.eventVersionOffset())
    );
  }


  @Override
  public Uni<List<Event>> fetch(EventStream eventStream) {
    // todo merge from all streams available streams
    throw new RedisEventStoreException(Es4jErrorBuilder.builder()
      .hint("not supported")
      .errorSource(ErrorSource.INFRASTRUCTURE)
      .cause("Redis error")
      .build()
    );
  }

  @Override
  public Uni<Void> stream(EventStream eventStream, Consumer<Event> consumer) {
    // todo merge from all streams available streams
    throw new RedisEventStoreException(Es4jErrorBuilder.builder()
      .hint("not supported")
      .errorSource(ErrorSource.INFRASTRUCTURE)
      .cause("Redis error")
      .build()
    );
  }


  private Event mapAttributes(Response response) {
    return response.toBuffer().toJsonObject().mapTo(Event.class);
  }

  @Override
  public Uni<Void> stop() {
    redisApi.close();
    return Uni.createFrom().voidItem();
  }

  @Override
  public void start(Es4jDeployment es4jDeployment, Vertx vertx, JsonObject configuration) {
    this.aggregateClass = es4jDeployment.aggregateClass();
    this.redisClient = Redis.createClient(vertx,
      new RedisOptions()
        .setMaxPoolSize(CpuCoreSensor.availableProcessors())
        .setMaxWaitingHandlers(CpuCoreSensor.availableProcessors() * 4)
        .setPassword(configuration.getString("redisPassword"))
        .setPoolName(camelToKebab(es4jDeployment.aggregateClass().getSimpleName()))
        .setConnectionString("redis://:%s@%s:%s/%s".formatted(
          configuration.getString("redisPassword"),
          configuration.getString("redisHost"),
          configuration.getString("redisPort"),
          configuration.getString("redisDb")
        ))
    );
    this.redisApi = RedisAPI.api(redisClient);
//    return redisClient.connect()
//      .replaceWithVoid();
  }

  @Override
  public Uni<Void> setup(Es4jDeployment aggregateClass, Vertx vertx, JsonObject configuration) {
    return null;
  }

  @Override
  public <T extends Aggregate> Uni<Void> trim(PruneEventStream<T> trim) {
    return redisApi.xtrim(List.of(aggregateStream(trim), "MINID", trim.offsetTo().toString()))
      .map(RedisEventStore::checkResponse)
      .replaceWithVoid();
  }
}
