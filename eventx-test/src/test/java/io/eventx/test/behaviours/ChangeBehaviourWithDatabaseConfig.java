package io.eventx.test.behaviours;


import io.eventx.Behaviour;
import io.eventx.Event;
import io.eventx.config.DBConfig;
import io.eventx.test.commands.ChangeDataWithDbConfig;
import io.eventx.test.domain.DataConfiguration;
import io.eventx.test.domain.FakeAggregate;
import io.eventx.test.events.DataChanged;

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
