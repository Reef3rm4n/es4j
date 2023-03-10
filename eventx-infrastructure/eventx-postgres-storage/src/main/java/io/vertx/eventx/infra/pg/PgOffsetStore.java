package io.vertx.eventx.infra.pg;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.infrastructure.OffsetStore;
import io.vertx.eventx.infra.pg.models.EventJournalOffSet;
import io.vertx.eventx.infra.pg.models.EventJournalOffSetKey;
import io.vertx.eventx.objects.JournalOffset;
import io.vertx.eventx.objects.JournalOffsetKey;
import io.vertx.eventx.sql.LiquibaseHandler;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.models.EmptyQuery;

public class PgOffsetStore implements OffsetStore {
  private final Repository<EventJournalOffSetKey, EventJournalOffSet, EmptyQuery> repository;

  public PgOffsetStore(Repository<EventJournalOffSetKey, EventJournalOffSet, EmptyQuery> repository) {
    this.repository = repository;
  }

  @Override
  public Uni<JournalOffset> put(JournalOffset journalOffset) {
    return null;
  }

  @Override
  public Uni<JournalOffset> get(JournalOffsetKey journalOffset) {
    return null;
  }

  @Override
  public Uni<Void> close() {
    return repository.repositoryHandler().close();
  }

  @Override
  public Uni<Void> start() {
    return LiquibaseHandler.runLiquibaseChangeLog(
      "pg-offset-store.xml",
      repository.repositoryHandler().vertx(),
      repository.repositoryHandler().configuration()
    );
  }
}
