package io.vertx.eventx.test.eventsourcing.domain;

import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.vertx.eventx.objects.EventxModule;
import io.vertx.eventx.test.eventsourcing.behaviours.ChangeBehaviour;
import io.vertx.eventx.test.eventsourcing.behaviours.ChangedAggregator;
import io.vertx.eventx.test.eventsourcing.behaviours.CreateAggregator;
import io.vertx.eventx.test.eventsourcing.behaviours.CreateBehaviour;

public class EventSourcingTestModule extends EventxModule {


  @Provides
  @Inject
  ChangedAggregator changeData1Aggregator() {
    return new ChangedAggregator();
  }

  @Provides
  @Inject
  ChangeBehaviour changeData1BehaviourEntity() {
    return new ChangeBehaviour();
  }

  @Provides
  @Inject
  CreateBehaviour createEntityBehaviour() {
    return new CreateBehaviour();
  }

  @Provides
  @Inject
  CreateAggregator entityBehaviour(){
    return new CreateAggregator();
  }

}
