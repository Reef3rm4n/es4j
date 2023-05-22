package io.eventx.test.behaviours;


import io.eventx.Behaviour;
import io.eventx.Event;
import io.eventx.test.domain.FakeAggregate;
import io.eventx.test.commands.ChangeData;
import io.eventx.test.events.DataChanged;

import java.util.List;

public class ChangeBehaviour implements Behaviour<FakeAggregate, ChangeData> {



  @Override
  public List<Event> process(final FakeAggregate state, final ChangeData command) {
    return List.of(new DataChanged(command.newData1()));
  }

}
