package io.es4j.infra.pg;

import com.google.auto.service.AutoService;
import io.es4j.Es4jDeployment;
import io.es4j.core.objects.OffsetBuilder;
import io.es4j.core.objects.OffsetKey;
import io.es4j.infra.pg.mappers.JournalOffsetMapper;
import io.es4j.infra.pg.models.EventJournalOffSet;
import io.es4j.infra.pg.models.EventJournalOffSetKey;
import io.es4j.infra.pg.models.EventJournalOffsetFilter;
import io.es4j.infrastructure.models.OffsetFilter;
import io.es4j.sql.LiquibaseHandler;
import io.es4j.sql.RepositoryHandler;
import io.es4j.sql.exceptions.IntegrityContraintViolation;
import io.es4j.sql.exceptions.NotFound;
import io.es4j.sql.models.QueryOptions;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.es4j.infrastructure.OffsetStore;
import io.es4j.core.objects.Offset;
import io.es4j.sql.Repository;

import io.es4j.sql.models.BaseRecord;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.es4j.core.CommandHandler.camelToKebab;


@AutoService(OffsetStore.class)
public class PgOffsetStore implements OffsetStore {
  private Repository<EventJournalOffSetKey, EventJournalOffSet, EventJournalOffsetFilter> repository;

  private final Logger LOGGER = LoggerFactory.getLogger(PgOffsetStore.class);

  @Override
  public Uni<Void> stop() {
    return repository.repositoryHandler().close();
  }

  @Override
  public void start(Es4jDeployment es4jDeployment, Vertx vertx, JsonObject config) {
    this.repository = new Repository<>(JournalOffsetMapper.INSTANCE, RepositoryHandler.leasePool(config, vertx));
  }

  @Override
  public Uni<Offset> put(Offset offset) {
    return repository.insert(getOffSet(offset))
      .onFailure(IntegrityContraintViolation.class)
      .recoverWithUni(repository.updateByKey(getOffSet(offset)))
      .map(PgOffsetStore::getJournalOffset);
  }


  private static EventJournalOffSet getOffSet(Offset offset) {
    return new EventJournalOffSet(
      offset.consumer(),
      offset.idOffSet(),
      offset.eventVersionOffset(),
      BaseRecord.newRecord(offset.tenantId())
    );
  }

  @Override
  public Uni<Offset> get(OffsetKey journalOffset) {
    return repository.selectByKey(new EventJournalOffSetKey(journalOffset.consumer(), journalOffset.tenantId()))
      .map(PgOffsetStore::getJournalOffset)
      .onFailure(NotFound.class).recoverWithUni(put(OffsetBuilder.builder()
          .eventVersionOffset(0L)
          .idOffSet(0L)
          .tenantId(journalOffset.tenantId())
          .consumer(journalOffset.consumer())
          .build()
        )
      );
  }

  @Override
  public Uni<Offset> reset(Offset offset) {
    return repository.updateByKey(getOffSet(offset)).map(PgOffsetStore::getJournalOffset);
  }

  @Override
  public Uni<List<Offset>> projections(OffsetFilter offsetFilter) {
    return repository.query(new EventJournalOffsetFilter(
        offsetFilter.consumers(),
        Objects.nonNull(offsetFilter.options()) ?
          new QueryOptions(
            null,
            offsetFilter.options().desc(),
            offsetFilter.options().creationDateFrom(),
            offsetFilter.options().creationDateTo(),
            offsetFilter.options().lastUpdateFrom(),
            offsetFilter.options().lastUpdateTo(),
            offsetFilter.options().pageNumber(),
            offsetFilter.options().pageSize(),
            null,
            offsetFilter.tenant()
          )
          :
          QueryOptions.simple(offsetFilter.tenant())

      ))
      .map(offsets -> offsets.stream().map(PgOffsetStore::getJournalOffset).toList());
  }

  private static Offset getJournalOffset(EventJournalOffSet offset) {
    return OffsetBuilder.builder()
      .consumer(offset.consumer())
      .eventVersionOffset(offset.eventVersionOffset())
      .idOffSet(offset.idOffSet())
      .tenantId(offset.baseRecord().tenant())
      .creationDate(offset.baseRecord().creationDate())
      .lastUpdate(offset.baseRecord().lastUpdate())
      .build();
  }

  @Override
  public Uni<Void> setup(Es4jDeployment es4jDeployment, Vertx vertx, JsonObject configuration) {
    final var schema = camelToKebab(es4jDeployment.aggregateClass().getSimpleName());
    LOGGER.debug("Migrating postgres schema {} configuration {}", schema, configuration);
    configuration.put("schema", schema);
    return LiquibaseHandler.liquibaseString(
      vertx,
      configuration,
      "pg-offset-store.xml",
      Map.of("schema", schema)
    );
  }

}
