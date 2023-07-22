package io.es4j.core.tasks;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.smallrye.mutiny.Uni;
import io.es4j.task.CronTask;
import io.es4j.task.CronTaskConfiguration;
import io.es4j.task.CronTaskConfigurationBuilder;

public class EventLogTrimmer implements CronTask {

  // todo implement a log compression mechanism that implements the following
  // 1) marks the id idOffset in the event-journal where it is safe to chop
  //  - first draft will determine the safety by querying the event-log for the lowest id for any given aggregateId stream where the Snapshot.class event is not present.
  //  - second draft will could the determine whe above for each existent aggregate
  // 2) add to the infrastructure an optional interface called EventStoreDump.class classes implementing this interface would handle offloading of eventTypes
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
