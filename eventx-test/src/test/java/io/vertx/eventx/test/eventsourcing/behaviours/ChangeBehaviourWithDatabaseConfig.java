package io.vertx.eventx.test.eventsourcing.behaviours;


import io.vertx.eventx.Behaviour;
import io.vertx.eventx.Event;
import io.vertx.eventx.config.DBConfig;
import io.vertx.eventx.test.eventsourcing.commands.ChangeDataWithDbConfig;
import io.vertx.eventx.test.eventsourcing.domain.DataConfiguration;
import io.vertx.eventx.test.eventsourcing.domain.FakeAggregate;
import io.vertx.eventx.test.eventsourcing.events.DataChanged;

import java.util.List;
import java.util.Objects;

public class ChangeBehaviourWithDatabaseConfig implements Behaviour<FakeAggregate, ChangeDataWithDbConfig> {

  private final DBConfig<DataConfiguration> dataConfiguration;

  public ChangeBehaviourWithDatabaseConfig(DBConfig<DataConfiguration> dataConfiguration) {
    this.dataConfiguration = dataConfiguration;
  }

  @Override
  public List<Event> process(final FakeAggregate state, final ChangeDataWithDbConfig command) {
    final var config = dataConfiguration.fetch(state.tenantID());
    Objects.requireNonNull(config.rule(), "configuration not present");
    return List.of(new DataChanged(command.newData()));
  }

}
