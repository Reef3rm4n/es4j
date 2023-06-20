package io.eventx.domain;

import com.google.auto.service.AutoService;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.eventx.behaviours.*;
import io.eventx.config.DatabaseBusinessRule;
import io.eventx.config.FileBusinessRule;
import io.eventx.core.objects.EventxModule;
import io.eventx.sql.RepositoryHandler;

@AutoService(EventxModule.class)
public class EventSourcingTestModule extends EventxModule {

  @Provides
  @Inject
  ChangedEventBehaviour changeData1Aggregator() {
    return new ChangedEventBehaviour();
  }

  @Provides
  @Inject
  ChangeCommandBehaviour changeData1BehaviourEntity() {
    return new ChangeCommandBehaviour();
  }

  @Provides
  @Inject
  ChangeCommandBehaviourWithConfiguration changeData1BehaviourEntity(FileBusinessRule<DataBusinessRule> dataConfiguration) {
    return new ChangeCommandBehaviourWithConfiguration(dataConfiguration);
  }

  @Provides
  @Inject
  ChangeCommandBehaviourWithDatabaseConfig changeData1BehaviourEntity(DatabaseBusinessRule<DataBusinessRule> dataConfiguration) {
    return new ChangeCommandBehaviourWithDatabaseConfig(dataConfiguration);
  }


  @Provides
  FileBusinessRule<DataBusinessRule> dataConfiguration() {
    return new FileBusinessRule<>(DataBusinessRule.class, "data-configuration");
  }

  @Provides
  DatabaseBusinessRule<DataBusinessRule> dataConfiguration(RepositoryHandler repositoryHandler) {
    return new DatabaseBusinessRule<>(DataBusinessRule.class, repositoryHandler);
  }

  @Provides
  @Inject
  CreateCommandBehaviour createEntityBehaviour() {
    return new CreateCommandBehaviour();
  }

  @Provides
  @Inject
  CreateEventBehaviour entityBehaviour() {
    return new CreateEventBehaviour();
  }

}
