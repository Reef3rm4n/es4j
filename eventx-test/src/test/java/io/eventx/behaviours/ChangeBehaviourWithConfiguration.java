package io.eventx.behaviours;


import com.google.auto.service.AutoService;
import io.eventx.Behaviour;
import io.eventx.Event;
import io.eventx.domain.DataFileBusinessRule;
import io.eventx.domain.FakeAggregate;
import io.eventx.events.DataChanged;
import io.eventx.commands.ChangeDataWithConfig;
import io.eventx.http.OpenApiDocs;
import io.eventx.infrastructure.config.FileBusinessRule;


import java.util.List;
import java.util.Objects;

@OpenApiDocs
@AutoService(Behaviour.class)
@SuppressWarnings("rawtypes")
public class ChangeBehaviourWithConfiguration implements Behaviour<FakeAggregate, ChangeDataWithConfig> {

  @Override
  public List<Event> process(final FakeAggregate state, final ChangeDataWithConfig command) {
    final var dataConfiguration = FileBusinessRule.get(DataFileBusinessRule.class, "data-configuration");
    Objects.requireNonNull(dataConfiguration.rule(), "configuration not present");
    return List.of(new DataChanged(command.newData()));
  }

}
