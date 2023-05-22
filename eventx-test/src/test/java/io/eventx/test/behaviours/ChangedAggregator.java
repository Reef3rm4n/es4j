package io.eventx.test.behaviours;


import io.eventx.Aggregator;
import io.eventx.test.domain.FakeAggregate;
import io.eventx.test.events.DataChanged;

public class ChangedAggregator implements Aggregator<FakeAggregate, DataChanged> {

  @Override
  public FakeAggregate apply(FakeAggregate aggregateState, DataChanged event) {
    return aggregateState.replaceData(event.newData());
  }




}
