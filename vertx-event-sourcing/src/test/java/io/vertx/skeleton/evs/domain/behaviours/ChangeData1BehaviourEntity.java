package io.vertx.skeleton.evs.domain.behaviours;

import io.vertx.skeleton.evs.Behaviour;
import io.vertx.skeleton.evs.domain.FakeEntity;
import io.vertx.skeleton.evs.domain.commands.ChangeData1;
import io.vertx.skeleton.evs.domain.events.Data1Changed;

import java.util.List;

public class ChangeData1BehaviourEntity implements Behaviour<FakeEntity, ChangeData1> {
  @Override
  public List<Object> process(final FakeEntity state, final ChangeData1 command) {
    return List.of(new Data1Changed(command.newData1()));
  }

}
