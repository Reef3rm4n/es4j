package io.es4j.behaviours;


import com.google.auto.service.AutoService;
import io.es4j.Behaviour;
import io.es4j.Event;
import io.es4j.config.DatabaseConfigurationFetcher;
import io.es4j.domain.FakeAggregate;
import io.es4j.events.DataChanged;
import io.es4j.saga.commands.ChangeDataWithDbConfig;
import io.es4j.domain.DataBusinessRule;
import io.es4j.http.OpenApiDocs;

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
