package io.vertx.eventx.infrastructure.pg;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;
import io.vertx.eventx.infrastructure.CommandStore;
import io.vertx.eventx.infrastructure.models.StoreCommand;
import io.vertx.eventx.infrastructure.pg.models.AggregateRecordKey;
import io.vertx.eventx.infrastructure.pg.models.RejectedCommand;
import io.vertx.eventx.sql.Repository;
import io.vertx.eventx.sql.models.*;

public class PgCommandStore implements CommandStore {

  private final Repository<AggregateRecordKey, RejectedCommand, EmptyQuery> storage;

  public PgCommandStore(Repository<AggregateRecordKey, RejectedCommand, EmptyQuery> repository) {
    this.storage = repository;
  }

  @Override
  public <T extends Aggregate, C extends Command> Uni<Void> save(StoreCommand<C, T> storeCommand) {
    return storage.insert(new RejectedCommand(
          storeCommand.entityId(),
          JsonObject.mapFrom(storeCommand.command()),
          storeCommand.command().getClass().getName(),
          storeCommand.error() != null ? JsonObject.mapFrom(storeCommand.error()) : null,
          BaseRecord.newRecord(storeCommand.command().headers().tenantId())
        )
      )
      .replaceWithVoid();
  }

}
