package io.eventx.behaviours;


import com.google.auto.service.AutoService;
import io.eventx.Behaviour;
import io.eventx.Event;
import io.eventx.domain.FakeAggregate;
import io.eventx.commands.ChangeData;
import io.eventx.events.DataChanged;
import io.eventx.http.OpenApiDocs;

import java.util.List;
@OpenApiDocs
@AutoService(Behaviour.class)
@SuppressWarnings("rawtypes")
public class ChangeBehaviour implements Behaviour<FakeAggregate, ChangeData> {


  public List<Event> process(final FakeAggregate state, final ChangeData command) {
    return List.of(
      new DataChanged(command.newData1())
    );
  }

}
