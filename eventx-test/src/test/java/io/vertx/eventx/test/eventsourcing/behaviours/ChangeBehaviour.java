package io.vertx.eventx.test.eventsourcing.behaviours;


import io.vertx.eventx.Behaviour;
import io.vertx.eventx.Event;
import io.vertx.eventx.config.FSConfig;
import io.vertx.eventx.test.eventsourcing.domain.DataConfiguration;
import io.vertx.eventx.test.eventsourcing.domain.FakeAggregate;
import io.vertx.eventx.test.eventsourcing.commands.ChangeData;
import io.vertx.eventx.test.eventsourcing.events.DataChanged;

import java.util.List;
import java.util.Objects;

public class ChangeBehaviour implements Behaviour<FakeAggregate, ChangeData> {



  @Override
  public List<Event> process(final FakeAggregate state, final ChangeData command) {
    return List.of(new DataChanged(command.newData1()));
  }

}
