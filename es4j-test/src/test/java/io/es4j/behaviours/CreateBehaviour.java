package io.es4j.behaviours;


import com.google.auto.service.AutoService;
import io.es4j.Behaviour;
import io.es4j.Event;
import io.es4j.saga.commands.CreateData;
import io.es4j.domain.FakeAggregate;
import io.es4j.events.DataCreated;
import io.es4j.http.OpenApiDocs;

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
