package io.eventx.test.behaviours;


import io.eventx.Behaviour;
import io.eventx.Event;
import io.eventx.config.FSConfig;
import io.eventx.test.commands.ChangeDataWithConfig;
import io.eventx.test.domain.DataConfiguration;
import io.eventx.test.domain.FakeAggregate;
import io.eventx.test.events.DataChanged;

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
