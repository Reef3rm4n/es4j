package io.eventx.behaviours;


import io.eventx.CommandBehaviour;
import io.eventx.Event;
import io.eventx.domain.FakeAggregate;
import io.eventx.commands.ChangeData;
import io.eventx.events.DataChanged;
import io.eventx.events.DataChanged2;
import io.eventx.events.DataChanged3;

import java.util.List;

public class ChangeCommandBehaviour implements CommandBehaviour<FakeAggregate, ChangeData> {



  @Override
  public List<Event> process(final FakeAggregate state, final ChangeData command) {
    return List.of(
      new DataChanged(command.newData1())
    );
  }

}
