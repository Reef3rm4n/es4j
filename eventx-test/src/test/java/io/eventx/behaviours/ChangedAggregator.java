package io.eventx.behaviours;


import io.eventx.Aggregator;
import io.eventx.domain.FakeAggregate;
import io.eventx.events.DataChanged;
public class ChangedAggregator implements Aggregator<FakeAggregate, DataChanged> {

  @Override
  public FakeAggregate apply(FakeAggregate aggregateState, DataChanged event) {
    return aggregateState.replaceData(event.newData());
  }




}
