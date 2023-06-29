package io.es4j.behaviours;


import com.google.auto.service.AutoService;
import io.es4j.Aggregator;
import io.es4j.domain.FakeAggregate;
import io.es4j.events.DataCreated;

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
