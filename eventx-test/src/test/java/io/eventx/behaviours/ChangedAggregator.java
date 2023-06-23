package io.eventx.behaviours;


import com.google.auto.service.AutoService;
import io.eventx.Aggregator;
import io.eventx.domain.FakeAggregate;
import io.eventx.events.DataChanged;


@AutoService(Aggregator.class)
@SuppressWarnings("rawtypes")
public class ChangedAggregator implements Aggregator<FakeAggregate, DataChanged> {

  @Override
  public FakeAggregate apply(FakeAggregate aggregateState, DataChanged event) {
    return aggregateState.replaceData(event.newData());
  }




}
