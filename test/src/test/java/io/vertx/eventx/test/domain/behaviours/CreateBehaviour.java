package io.vertx.eventx.test.domain.behaviours;


import io.vertx.eventx.Behaviour;
import io.vertx.eventx.test.domain.FakeAggregate;
import io.vertx.eventx.test.domain.commands.CreateData;
import io.vertx.eventx.test.domain.events.DataCreated;

import java.util.List;

public class CreateBehaviour implements Behaviour<FakeAggregate, CreateData> {
  @Override
  public List<Object> process(FakeAggregate state, CreateData command) {
    return List.of(new DataCreated(command.entityId(), command.data()));
  }

}
