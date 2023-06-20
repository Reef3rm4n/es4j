package io.eventx.behaviours;


import io.eventx.Behaviour;
import io.eventx.Event;
import io.eventx.commands.CreateData;
import io.eventx.domain.FakeAggregate;
import io.eventx.events.DataCreated;

import java.util.List;

public class CreateBehaviour implements Behaviour<FakeAggregate, CreateData> {
  @Override
  public List<Event> process(FakeAggregate state, CreateData command) {
    return List.of(new DataCreated(command.aggregateId(), command.data()));
  }

}
