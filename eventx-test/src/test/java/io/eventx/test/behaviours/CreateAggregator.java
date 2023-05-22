package io.eventx.test.behaviours;


import io.eventx.Aggregator;
import io.eventx.test.domain.FakeAggregate;
import io.eventx.test.events.DataCreated;

public class CreateAggregator implements Aggregator<FakeAggregate, DataCreated> {
  @Override
  public FakeAggregate apply(FakeAggregate aggregateState, DataCreated event) {
   return new FakeAggregate(
      event.entityId(),
      event.data()
    );
  }
}
