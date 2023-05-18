package io.vertx.eventx.test.eventsourcing.behaviours;


import io.vertx.eventx.Behaviour;
import io.vertx.eventx.Event;
import io.vertx.eventx.config.FSConfig;
import io.vertx.eventx.test.eventsourcing.commands.ChangeDataWithConfig;
import io.vertx.eventx.test.eventsourcing.domain.DataConfiguration;
import io.vertx.eventx.test.eventsourcing.domain.FakeAggregate;
import io.vertx.eventx.test.eventsourcing.events.DataChanged;

import java.util.List;
import java.util.Objects;

public class ChangeBehaviourWithConfiguration implements Behaviour<FakeAggregate, ChangeDataWithConfig> {

  private final FSConfig<DataConfiguration> dataConfiguration;

  public ChangeBehaviourWithConfiguration(FSConfig<DataConfiguration> dataConfiguration) {
    this.dataConfiguration = dataConfiguration;
  }

  @Override
  public List<Event> process(final FakeAggregate state, final ChangeDataWithConfig command) {
    Objects.requireNonNull(dataConfiguration.get().rule(), "configuration not present");
    return List.of(new DataChanged(command.newData()));
  }

}
