package io.es4j;

import com.cronutils.model.Cron;

public interface CronCommand<C extends Command> {

  C command();

  Cron cron();

}
