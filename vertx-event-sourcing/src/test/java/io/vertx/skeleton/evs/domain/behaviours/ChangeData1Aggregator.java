package io.vertx.skeleton.evs.domain.behaviours;

import io.vertx.skeleton.evs.Aggregator;
import io.vertx.skeleton.evs.domain.FakeEntity;
import io.vertx.skeleton.evs.domain.events.Data1Changed;

public class ChangeData1Aggregator implements Aggregator<FakeEntity, Data1Changed > {

  @Override
  public FakeEntity apply(FakeEntity aggregateState, Data1Changed event) {
    return aggregateState.changeData1(event.newData1());
  }


}
