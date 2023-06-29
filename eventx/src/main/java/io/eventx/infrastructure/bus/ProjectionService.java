package io.eventx.infrastructure.bus;

import io.eventx.Aggregate;
import io.eventx.core.exceptions.EventxException;
import io.eventx.core.objects.EventxError;
import io.eventx.core.objects.JournalOffsetBuilder;
import io.eventx.core.objects.JournalOffsetKey;
import io.eventx.infrastructure.EventStore;
import io.eventx.infrastructure.OffsetStore;
import io.eventx.infrastructure.models.EventStreamBuilder;
import io.eventx.infrastructure.models.ProjectionStream;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.Message;

import java.util.List;
import java.util.Objects;

import static io.eventx.core.CommandHandler.camelToKebab;

public class ProjectionService {

  private final OffsetStore offsetStore;
  private final EventStore eventStore;
  private final Class<? extends Aggregate> aClass;

  public ProjectionService(OffsetStore offsetStore, EventStore eventStore, Class<? extends Aggregate> aClass) {
    this.offsetStore = offsetStore;
    this.eventStore = eventStore;
    this.aClass = aClass;
  }

  public Uni<Void> register(Vertx vertx) {
    return vertx.eventBus().<JsonObject>consumer(nextAddress(aClass))
      .handler(
        message -> next(message.body().mapTo(ProjectionStream.class))
          .subscribe()
          .with(
            events -> message.reply(new JsonArray(events)),
            throwable -> handleThrowable(message, throwable)
          )
      )
      .exceptionHandler(this::handle)
      .completionHandler()
      .flatMap(avoid -> vertx.eventBus().<JsonObject>consumer(resetAddress(aClass))
        .handler(
          message -> reset(
            message.body().getString("projectionId"),
            message.body().getString("tenant", "default"),
            message.body().getLong("idOffset", 0L)
          )
            .subscribe()
            .with(
              events -> message.reply("Void.class"),
              throwable -> handleThrowable(message, throwable)
            )
        )
        .exceptionHandler(this::handle)
        .completionHandler()
      );
  }

  public static String resetAddress(Class<? extends Aggregate> aClass) {
    return "%s/projection/reset".formatted(camelToKebab(aClass.getSimpleName()));
  }

  public static String nextAddress(Class<? extends Aggregate> aClass) {
    return "%s/projection/next".formatted(camelToKebab(aClass.getSimpleName()));
  }

  private static void handleThrowable(Message<JsonObject> message, Throwable throwable) {
    if (throwable instanceof EventxException vertxServiceException) {
      message.fail(vertxServiceException.error().externalErrorCode(), JsonObject.mapFrom(vertxServiceException.error()).encodePrettily());
    } else {
      message.fail(500, JsonObject.mapFrom(new EventxError(throwable.getMessage(), throwable.getLocalizedMessage(), 500)).encode());
    }
  }

  private void handle(Throwable throwable) {

  }

  public Uni<List<io.eventx.infrastructure.models.Event>> next(ProjectionStream projectionStream) {
    return offsetStore.get(new JournalOffsetKey(projectionStream.projectionId(), projectionStream.tenantId()))
      .flatMap(journalOffset -> eventStore.fetch(
          EventStreamBuilder.builder()
            .offset(journalOffset.idOffSet())
            .batchSize(projectionStream.batchSize())
            .tenantId(projectionStream.tenantId())
            .tags(projectionStream.tags())
            .to(projectionStream.to())
            .from(projectionStream.from())
            .versionFrom(projectionStream.versionFrom())
            .versionTo(projectionStream.versionTo())
            .aggregateIds(projectionStream.aggregateIds())
            .build()
        )
      );
  }

  public Uni<Void> reset(String projectionId, String tenant, Long idOffset) {
    return offsetStore.get(new JournalOffsetKey(projectionId, tenant))
      .flatMap(journalOffset -> offsetStore.put(
          JournalOffsetBuilder.builder(journalOffset)
            .idOffSet(Objects.requireNonNullElse(idOffset, 0L))
            .build()
        )
      )
      .replaceWithVoid();
  }


}
