package io.eventx.behaviours;


import com.google.auto.service.AutoService;
import io.eventx.Aggregator;
import io.eventx.domain.FakeAggregate;
import io.eventx.events.DataCreated;

@AutoService(Aggregator.class)
@SuppressWarnings("rawtypes")
public class CreateAggregator implements Aggregator<FakeAggregate, DataCreated> {
  @Override
  public FakeAggregate apply(FakeAggregate aggregateState, DataCreated event) {
   return new FakeAggregate(
      event.entityId(),
      event.data()
    );
  }
}
