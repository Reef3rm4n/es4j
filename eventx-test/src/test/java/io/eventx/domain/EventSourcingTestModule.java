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
  ChangeBehaviourWithConfiguration changeData1BehaviourEntity(FileBusinessRule<DataBusinessRule> dataConfiguration) {
    return new ChangeBehaviourWithConfiguration(dataConfiguration);
  }

  @Provides
  @Inject
  ChangeBehaviourWithDatabaseConfig changeData1BehaviourEntity(DatabaseBusinessRule<DataBusinessRule> dataConfiguration) {
    return new ChangeBehaviourWithDatabaseConfig(dataConfiguration);
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
  CreateBehaviour createEntityBehaviour() {
    return new CreateBehaviour();
  }

  @Provides
  @Inject
  CreateAggregator entityBehaviour() {
    return new CreateAggregator();
  }

}
