package io.eventx.behaviours;


import io.eventx.Behaviour;
import io.eventx.Event;
import io.eventx.config.DatabaseBusinessRule;
import io.eventx.domain.FakeAggregate;
import io.eventx.events.DataChanged;
import io.eventx.commands.ChangeDataWithDbConfig;
import io.eventx.domain.DataBusinessRule;
import io.eventx.http.OpenApiDocs;

import java.util.List;
@OpenApiDocs
public class ChangeBehaviourWithDatabaseConfig implements Behaviour<FakeAggregate, ChangeDataWithDbConfig> {

  private final DatabaseBusinessRule<DataBusinessRule> dataConfiguration;

  public ChangeBehaviourWithDatabaseConfig(DatabaseBusinessRule<DataBusinessRule> dataConfiguration) {
    this.dataConfiguration = dataConfiguration;
  }

  @Override
  public List<Event> process(final FakeAggregate state, final ChangeDataWithDbConfig command) {
    final var config = dataConfiguration.fetch(state.tenant());
    config.orElseThrow(() -> new IllegalStateException("configuration not found"));
    return List.of(new DataChanged(command.newData()));
  }

}
