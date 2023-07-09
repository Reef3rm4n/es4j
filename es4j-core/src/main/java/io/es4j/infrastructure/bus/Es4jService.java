package io.es4j.infrastructure.bus;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import io.es4j.Aggregate;
import io.es4j.Event;
import io.es4j.core.exceptions.Es4jException;
import io.es4j.core.objects.*;
import io.es4j.infrastructure.EventStore;
import io.es4j.infrastructure.OffsetStore;
import io.es4j.infrastructure.misc.EventParser;
import io.es4j.infrastructure.models.*;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static io.es4j.core.CommandHandler.camelToKebab;

public class Es4jService {

  private final OffsetStore offsetStore;
  private final EventStore eventStore;
  private final Class<? extends Aggregate> aClass;

  protected static final Logger LOGGER = LoggerFactory.getLogger(Es4jService.class);
  private final Set<Class> events;

  private final Map<String, JsonNode> commandSchemas;


  public Es4jService(OffsetStore offsetStore, EventStore eventStore, Class<? extends Aggregate> aClass, List<AggregatorWrap> aggregatorWraps, List<BehaviourWrap> behaviourWraps) {
    this.offsetStore = offsetStore;
    this.eventStore = eventStore;
    this.aClass = aClass;
    SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_6, OptionPreset.PLAIN_JSON);
    SchemaGeneratorConfig config = configBuilder.build();
    SchemaGenerator generator = new SchemaGenerator(config);
    commandSchemas = behaviourWraps.stream().map(BehaviourWrap::commandClass)
      .map(commandClass -> {
        try {
          generator.generateSchema(commandClass);
          return Tuple2.of(camelToKebab(commandClass.getSimpleName()), generator.generateSchema(commandClass));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      })
      .collect(Collectors.toMap(Tuple2::getItem1, Tuple2::getItem2));
    this.events = aggregatorWraps.stream().map(AggregatorWrap::eventClass).collect(Collectors.toSet());
  }

  public Uni<Void> register(Vertx vertx) {
    return vertx.eventBus().<JsonObject>consumer(fetchNextEventsAddress(aClass))
      .handler(
        message -> fetchNextEvents(message.body().mapTo(FetchNextEvents.class))
          .subscribe()
          .with(
            events -> message.reply(new JsonArray(events)),
            throwable -> handleThrowable(message, throwable)
          )
      )
      .exceptionHandler(this::handle)
      .completionHandler()
      .flatMap(avoid -> vertx.eventBus().<JsonObject>consumer(resetOffsetAddress(aClass))
        .handler(
          message -> reset(
            message.body().getString("projectionId"),
            message.body().getString("tenant", "default"),
            message.body().getLong("idOffset", 0L)
          )
            .subscribe()
            .with(
              events -> message.reply("void"),
              throwable -> handleThrowable(message, throwable)
            )
        )
        .exceptionHandler(this::handle)
        .completionHandler()
        .flatMap(av -> vertx.eventBus().<JsonObject>consumer(fetchEventsAddress(aClass))
          .handler(
            message -> fetchEvents(message.body().mapTo(EventFilter.class))
              .subscribe()
              .with(
                events -> message.reply(new JsonArray(events)),
                throwable -> handleThrowable(message, throwable)
              )
          )
          .exceptionHandler(this::handle)
          .completionHandler()
        )
        .flatMap(av -> vertx.eventBus().<JsonObject>consumer(fetchOffsetsAddress(aClass))
          .handler(
            message -> offsets(message.body().mapTo(OffsetFilter.class))
              .subscribe()
              .with(
                offsets -> message.reply(new JsonArray(offsets)),
                throwable -> handleThrowable(message, throwable)
              )
          )
          .exceptionHandler(this::handle)
          .completionHandler()
        )
        .flatMap(av -> vertx.eventBus().<JsonObject>consumer(availableTypes(aClass))
          .handler(
            message -> message.reply(JsonObject.mapFrom(new AvailableTypes(
                  events.stream().map(Class::getName).toList(),
                  commandSchemas
                )
              )
            )
          )
          .exceptionHandler(this::handle)
          .completionHandler()
        )
      );
  }
  public static String resetOffsetAddress(Class<? extends Aggregate> aClass) {
    return "/%s/event-consumer/reset/offset".formatted(camelToKebab(aClass.getSimpleName()));
  }

  public static String fetchNextEventsAddress(Class<? extends Aggregate> aClass) {
    return "/%s/event-consumer/next".formatted(camelToKebab(aClass.getSimpleName()));
  }

  public static String fetchEventsAddress(Class<? extends Aggregate> aClass) {
    return "/%s/event".formatted(camelToKebab(aClass.getSimpleName()));
  }

  public static String fetchOffsetsAddress(Class<? extends Aggregate> aClass) {
    return "/%s/event-consumers".formatted(camelToKebab(aClass.getSimpleName()));
  }

  public static String availableTypes(Class<? extends Aggregate> aClass) {
    return "/%s/available-types".formatted(camelToKebab(aClass.getSimpleName()));
  }

  private static void handleThrowable(Message<JsonObject> message, Throwable throwable) {
    if (throwable instanceof Es4jException vertxServiceException) {
      message.fail(vertxServiceException.error().externalErrorCode(), JsonObject.mapFrom(vertxServiceException.error()).encode());
    } else {
      message.fail(500, JsonObject.mapFrom(new Es4jError(throwable.getMessage(), Objects.nonNull(throwable.getCause()) ? throwable.getCause().getMessage() : throwable.getLocalizedMessage(), 500)).encode());
    }
  }

  private void handle(Throwable throwable) {
    LOGGER.error("Unhandled exception", throwable);
  }

  public Uni<List<io.es4j.infrastructure.models.Event>> fetchNextEvents(FetchNextEvents eventStreamQuery) {
    return offsetStore.get(new OffsetKey(eventStreamQuery.projectionId(), eventStreamQuery.tenantId()))
      .flatMap(journalOffset -> eventStore.fetch(
            EventStreamBuilder.builder()
              .offset(journalOffset.idOffSet())
              .batchSize(eventStreamQuery.batchSize())
              .tenantId(eventStreamQuery.tenantId())
              .tags(eventStreamQuery.tags())
              .aggregateIds(eventStreamQuery.aggregateIds())
              .build()
          )
          .flatMap(events -> offsetStore.put(journalOffset.updateOffset(events))
            .replaceWith(events)
          )
      );
  }

  public Uni<List<io.es4j.infrastructure.models.Event>> fetchEvents(EventFilter eventFilter) {
    return eventStore.fetch(
      EventStreamBuilder.builder()
        .offset(eventFilter.offset())
        .batchSize(eventFilter.batchSize())
        .tenantId(eventFilter.tenantId())
        .tags(eventFilter.tags())
        .to(eventFilter.to())
        .from(eventFilter.from())
        .events(figureEventClass(eventFilter.events()))
        .versionFrom(eventFilter.versionFrom())
        .versionTo(eventFilter.versionTo())
        .aggregateIds(eventFilter.aggregateIds())
        .build()
    );
  }

  private List<Class<? extends Event>> figureEventClass(List<String> classNames) {
    final var arrayList = new ArrayList<Class<? extends Event>>();
    classNames.forEach(
      className -> {
        try {
          arrayList.add((Class<? extends Event>) Class.forName(className));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    );
   return arrayList;
  }

  public Uni<List<Offset>> offsets(OffsetFilter offsetFilter) {
    return offsetStore.projections(offsetFilter);
  }

  public Uni<Void> reset(String projectionId, String tenant, Long idOffset) {
    return offsetStore.get(new OffsetKey(projectionId, tenant))
      .flatMap(journalOffset -> offsetStore.put(
          OffsetBuilder.builder(journalOffset)
            .idOffSet(Objects.requireNonNullElse(idOffset, 0L))
            .build()
        )
      )
      .replaceWithVoid();
  }


}
