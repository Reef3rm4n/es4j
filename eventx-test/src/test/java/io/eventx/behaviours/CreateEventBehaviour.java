package io.eventx.behaviours;


import io.eventx.EventBehaviour;
import io.eventx.domain.FakeAggregate;
import io.eventx.events.DataCreated;

public class CreateEventBehaviour implements EventBehaviour<FakeAggregate, DataCreated> {
  @Override
  public FakeAggregate apply(FakeAggregate aggregateState, DataCreated event) {
   return new FakeAggregate(
      event.entityId(),
      event.data()
    );
  }
}
