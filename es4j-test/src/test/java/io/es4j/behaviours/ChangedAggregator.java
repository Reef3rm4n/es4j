package io.es4j.behaviours;


import com.google.auto.service.AutoService;
import io.es4j.Aggregator;
import io.es4j.domain.FakeAggregate;
import io.es4j.events.DataChanged;


@AutoService(Aggregator.class)
@SuppressWarnings("rawtypes")
public class ChangedAggregator implements Aggregator<FakeAggregate, DataChanged> {

  @Override
  public FakeAggregate apply(FakeAggregate aggregateState, DataChanged event) {
    return aggregateState.replaceData(event.newData());
  }




}
