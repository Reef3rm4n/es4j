package io.vertx.eventx.core.tasks;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.smallrye.mutiny.Uni;
import io.vertx.eventx.task.CronTask;
import io.vertx.eventx.task.CronTaskConfiguration;
import io.vertx.eventx.task.CronTaskConfigurationBuilder;

public class EventStoreCompression implements CronTask {

  // todo implement a log compression mechanism that implements the following
  // 1) marks the id offset in the event-journal where it is safe to chop
  //  - first draft will determine the safety by querying the event-log for the lowest id for any given aggregateId stream where the Snapshot.class event is not present.
  //  - second draft will could the determine whe above for each existent aggregate
  // 2) add to the infrastructure an optional interface called EventStoreDump.class classes implementing this interface would handle offloading of events
  //  - implementations would have to be idempotent
  // 3) adds chopping process related methods to the EventStore.class interface
  //  - this must be called from a single actor during its entire duration
  //  - eviction must be done so that the EventStoreDump.class is guaranteed to be successfully called at least once

  @Override
  public Uni<Void> performTask() {
    return null;
  }

  @Override
  public CronTaskConfiguration configuration() {
    return CronTaskConfigurationBuilder.builder()
      .cron(new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)).parse("0 0 * * *"))
      .build();
  }
}
