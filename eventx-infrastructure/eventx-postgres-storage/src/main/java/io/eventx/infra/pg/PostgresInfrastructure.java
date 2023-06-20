package io.eventx.infra.pg;

import com.google.auto.service.AutoService;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.eventx.core.objects.EventxModule;
import io.eventx.infra.pg.models.*;
import io.eventx.infrastructure.EventStore;
import io.eventx.infrastructure.OffsetStore;
import io.eventx.sql.models.EmptyQuery;
import io.vertx.core.json.JsonObject;
import io.eventx.infra.pg.mappers.EventStoreMapper;
import io.eventx.infra.pg.mappers.JournalOffsetMapper;
import io.eventx.sql.Repository;
import io.eventx.sql.RepositoryHandler;
import io.vertx.mutiny.core.Vertx;


@AutoService(EventxModule.class)
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
  Repository<EventRecordKey, EventRecord, EventRecordQuery> eventJournal(RepositoryHandler repositoryHandler) {
    return new Repository<>(EventStoreMapper.INSTANCE, repositoryHandler);
  }

  @Provides
  @Inject
  Repository<EventJournalOffSetKey, EventJournalOffSet, EmptyQuery> journalOffset(RepositoryHandler repositoryHandler) {
    return new Repository<>(JournalOffsetMapper.INSTANCE, repositoryHandler);
  }

  @Provides
  @Inject
  RepositoryHandler repositoryHandler(Vertx vertx, JsonObject config) {
    return RepositoryHandler.leasePool(config, vertx);
  }

}
