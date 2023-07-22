package io.es4j.behaviours;


import com.google.auto.service.AutoService;
import io.es4j.Behaviour;
import io.es4j.Event;
import io.es4j.domain.DataFileBusinessRule;
import io.es4j.domain.FakeAggregate;
import io.es4j.events.DataChanged;
import io.es4j.commands.ChangeDataWithConfig;
import io.es4j.http.OpenApiDocs;
import io.es4j.infrastructure.config.FileConfiguration;


import java.util.List;
import java.util.Objects;

@OpenApiDocs
@AutoService(Behaviour.class)
@SuppressWarnings("rawtypes")
public class ChangeBehaviourWithConfiguration implements Behaviour<FakeAggregate, ChangeDataWithConfig> {

  @Override
  public List<Event> process(final FakeAggregate state, final ChangeDataWithConfig command) {
    final var dataConfiguration = FileConfiguration.get(DataFileBusinessRule.class, "data-configuration");
    Objects.requireNonNull(dataConfiguration.rule(), "configuration not present");
    return List.of(new DataChanged(command.newData()));
  }

}
