package io.vertx.eventx.infra.pg;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infra.pg.mappers.JournalOffsetMapper;
import io.vertx.eventx.infra.pg.mappers.OffsetMapper;
import io.vertx.eventx.infrastructure.OffsetStore;
import io.vertx.eventx.infra.pg.models.EventJournalOffSet;
import io.vertx.eventx.infra.pg.models.EventJournalOffSetKey;
import io.vertx.eventx.objects.JournalOffset;
import io.vertx.eventx.objects.JournalOffsetBuilder;
import io.vertx.eventx.objects.JournalOffsetKey;
import io.vertx.eventx.sql.LiquibaseHandler;
import io.vertx.eventx.sql.Repository;

import io.vertx.eventx.sql.exceptions.IntegrityContraintViolation;
import io.vertx.eventx.sql.exceptions.NotFound;
import io.vertx.eventx.sql.models.BaseRecord;
import io.vertx.eventx.sql.models.EmptyQuery;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.vertx.eventx.core.AggregateVerticleLogic.camelToKebab;

public class PgOffsetStore implements OffsetStore {
  private final Repository<EventJournalOffSetKey, EventJournalOffSet, EmptyQuery> repository;

  private final Logger LOGGER = LoggerFactory.getLogger(PgOffsetStore.class);


  public PgOffsetStore(Repository<EventJournalOffSetKey, EventJournalOffSet, EmptyQuery> repository) {
    this.repository = repository;
  }

  @Override
  public Uni<JournalOffset> put(JournalOffset journalOffset) {
    return repository.insert(getOffSet(journalOffset))
      .onFailure(IntegrityContraintViolation.class)
      .recoverWithUni(repository.updateByKey(getOffSet(journalOffset)))
      .map(PgOffsetStore::getJournalOffset);
  }


  private static EventJournalOffSet getOffSet(JournalOffset journalOffset) {
    return new EventJournalOffSet(
      journalOffset.consumer(),
      journalOffset.idOffSet(),
      journalOffset.eventVersionOffset(),
      BaseRecord.newRecord(journalOffset.tenantId())
    );
  }

  @Override
  public Uni<JournalOffset> get(JournalOffsetKey journalOffset) {
    // todo if not present create
    return repository.selectByKey(new EventJournalOffSetKey(journalOffset.consumer(), journalOffset.tenantId()))
      .map(PgOffsetStore::getJournalOffset)
      .onFailure(NotFound.class).recoverWithUni(put(JournalOffsetBuilder.builder()
          .eventVersionOffset(0L)
          .idOffSet(0L)
          .tenantId(journalOffset.tenantId())
          .consumer(journalOffset.consumer())
          .build()
        )
      );
  }


  private static JournalOffset getJournalOffset(EventJournalOffSet offset) {
    return new JournalOffset(
      offset.consumer(),
      offset.baseRecord().tenantId(),
      offset.idOffSet(),
      offset.eventVersionOffset()
    );
  }

  @Override
  public Uni<Void> close() {
    return repository.repositoryHandler().close();
  }

  @Override
  public Uni<Void> start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration) {
    LOGGER.debug("Migrating database for {} with configuration {}", aggregateClass.getSimpleName(), configuration);
    return LiquibaseHandler.liquibaseString(
      repository.repositoryHandler(),
      "pg-offset-store.xml",
      Map.of("schema", camelToKebab(aggregateClass.getSimpleName()))
    );
  }
}
