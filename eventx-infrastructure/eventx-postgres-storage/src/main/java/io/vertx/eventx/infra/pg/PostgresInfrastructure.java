package io.vertx.eventx.infra.pg;

import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.infra.pg.mappers.EventJournalMapper;
import io.vertx.eventx.infra.pg.mappers.JournalOffsetMapper;
import io.vertx.eventx.infra.pg.models.*;
import io.vertx.eventx.infrastructure.*;
import io.vertx.eventx.objects.EventxModule;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.RepositoryHandler;
import io.vertx.eventx.sql.models.EmptyQuery;
import io.vertx.mutiny.core.Vertx;

public class PostgresInfrastructure extends EventxModule {

  @Provides
  @Inject
  EventStore eventStore(
    final Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal
  ) {
    return new PgEventStore(eventJournal);
  }

  @Provides
  @Inject
  OffsetStore offsetStore(
    Repository<EventJournalOffSetKey, EventJournalOffSet, EmptyQuery> repository
  ) {
    return new PgOffsetStore(repository);
  }
  @Provides
  @Inject
  RepositoryHandler repositoryHandler(JsonObject configuration, Vertx vertx) {
    return RepositoryHandler.leasePool(configuration, vertx);
  }

  @Provides
  @Inject
  Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal(RepositoryHandler repositoryHandler) {
    return new Repository<>(EventJournalMapper.INSTANCE, repositoryHandler);
  }

  @Provides
  @Inject
  Repository<EventJournalOffSetKey, EventJournalOffSet, EmptyQuery> journalOffset(RepositoryHandler repositoryHandler) {
    return new Repository<>(JournalOffsetMapper.INSTANCE, repositoryHandler);
  }

}
