package io.eventx.behaviours;


import com.google.auto.service.AutoService;
import io.eventx.Behaviour;
import io.eventx.Event;
import io.eventx.commands.CreateData;
import io.eventx.domain.FakeAggregate;
import io.eventx.events.DataCreated;
import io.eventx.http.OpenApiDocs;

import java.util.List;



@OpenApiDocs
@AutoService(Behaviour.class)
@SuppressWarnings("rawtypes")
public class CreateBehaviour implements Behaviour<FakeAggregate, CreateData> {
  @Override
  public List<Event> process(FakeAggregate state, CreateData command) {
    return List.of(new DataCreated(command.aggregateId(), command.data()));
  }

}
