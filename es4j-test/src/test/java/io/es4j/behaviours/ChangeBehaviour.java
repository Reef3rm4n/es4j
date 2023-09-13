package io.es4j.behaviours;


import com.google.auto.service.AutoService;
import io.es4j.Behaviour;
import io.es4j.Event;
import io.es4j.domain.FakeAggregate;
import io.es4j.saga.commands.ChangeData;
import io.es4j.events.DataChanged;
import io.es4j.http.OpenApiDocs;

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
