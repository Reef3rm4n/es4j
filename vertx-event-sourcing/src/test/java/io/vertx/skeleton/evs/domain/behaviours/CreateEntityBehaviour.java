package io.vertx.skeleton.evs.domain.behaviours;

import io.vertx.skeleton.evs.Behaviour;
import io.vertx.skeleton.evs.Aggregator;
import io.vertx.skeleton.evs.domain.FakeEntity;
import io.vertx.skeleton.evs.domain.commands.Create;
import io.vertx.skeleton.evs.domain.events.EntityCreated;

import java.util.List;

public class CreateEntityBehaviour implements Behaviour<FakeEntity, Create>, Aggregator<FakeEntity, EntityCreated> {
  @Override
  public List<Object> process(FakeEntity state, Create command) {
    return List.of(new EntityCreated(command.entityId()));
  }

  @Override
  public FakeEntity apply(FakeEntity aggregateState, EntityCreated event) {
    return aggregateState.create(event.entityId());
  }

}
