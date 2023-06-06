package io.eventx.behaviours;


import io.eventx.EventBehaviour;
import io.eventx.domain.FakeAggregate;
import io.eventx.events.DataChanged;

public class ChangedEventBehaviour implements EventBehaviour<FakeAggregate, DataChanged> {

  @Override
  public FakeAggregate apply(FakeAggregate aggregateState, DataChanged event) {
    return aggregateState.replaceData(event.newData());
  }




}
