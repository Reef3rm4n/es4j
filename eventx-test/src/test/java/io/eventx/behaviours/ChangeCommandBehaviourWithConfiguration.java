package io.eventx.behaviours;


import io.eventx.CommandBehaviour;
import io.eventx.Event;
import io.eventx.config.FileBusinessRule;
import io.eventx.domain.FakeAggregate;
import io.eventx.events.DataChanged;
import io.eventx.commands.ChangeDataWithConfig;
import io.eventx.domain.DataBusinessRule;

import java.util.List;
import java.util.Objects;

public class ChangeCommandBehaviourWithConfiguration implements CommandBehaviour<FakeAggregate, ChangeDataWithConfig> {

  private final FileBusinessRule<DataBusinessRule> dataConfiguration;

  public ChangeCommandBehaviourWithConfiguration(FileBusinessRule<DataBusinessRule> dataConfiguration) {
    this.dataConfiguration = dataConfiguration;
  }

  @Override
  public List<Event> process(final FakeAggregate state, final ChangeDataWithConfig command) {
    Objects.requireNonNull(dataConfiguration.get().rule(), "configuration not present");
    return List.of(new DataChanged(command.newData()));
  }

}
