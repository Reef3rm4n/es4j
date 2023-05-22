package io.eventx.task;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import io.eventx.sql.exceptions.NotFound;
import io.smallrye.mutiny.Uni;

import java.util.List;

public interface CronTask {

  Uni<Void> performTask();

  default CronTaskConfiguration configuration() {
    return new CronTaskConfiguration(
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)).parse("0 0 * * *"),
      LockLevel.LOCAL,
      List.of(NotFound.class)
    );
  }


}
