package io.vertx.eventx.domain.behaviours;


import io.vertx.eventx.Aggregator;
import io.vertx.eventx.domain.FakeAggregate;
import io.vertx.eventx.domain.events.DataCreated;

public class CreateAggregator implements Aggregator<FakeAggregate, DataCreated> {
  @Override
  public FakeAggregate apply(FakeAggregate aggregateState, DataCreated event) {
   return new FakeAggregate(
      event.entityId(),
      event.data()
    );
  }
}
