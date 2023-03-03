package io.vertx.eventx.infrastructure;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.Command;
import io.vertx.eventx.infrastructure.models.StoreCommand;

public interface CommandStore {

  <T extends Aggregate, C extends Command> Uni<Void> save(StoreCommand<C, T> storeCommand);

}
