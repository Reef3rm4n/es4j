package io.eventx.behaviours;


import com.google.auto.service.AutoService;
import io.eventx.Behaviour;
import io.eventx.Event;
import io.eventx.config.DatabaseConfigurationFetcher;
import io.eventx.domain.FakeAggregate;
import io.eventx.events.DataChanged;
import io.eventx.commands.ChangeDataWithDbConfig;
import io.eventx.domain.DataBusinessRule;
import io.eventx.http.OpenApiDocs;

import java.util.List;
@OpenApiDocs
@AutoService(Behaviour.class)
public class ChangeBehaviourWithDatabaseConfig implements Behaviour<FakeAggregate, ChangeDataWithDbConfig> {

  @Override
  public List<Event> process(final FakeAggregate state, final ChangeDataWithDbConfig command) {
    final var config = DatabaseConfigurationFetcher.fetch(DataBusinessRule.class);
    config.orElseThrow(() -> new IllegalStateException("configuration not found"));
    return List.of(new DataChanged(command.newData()));
  }

}
