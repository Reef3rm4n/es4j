package io.eventx.test.domain;

import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.eventx.config.DBConfig;
import io.eventx.config.FSConfig;
import io.eventx.core.objects.EventxModule;
import io.eventx.test.behaviours.*;
import io.eventx.sql.RepositoryHandler;

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
  ChangeBehaviourWithConfiguration changeData1BehaviourEntity(FSConfig<DataConfiguration> dataConfiguration) {
    return new ChangeBehaviourWithConfiguration(dataConfiguration);
  }

  @Provides
  @Inject
  ChangeBehaviourWithDatabaseConfig changeData1BehaviourEntity(DBConfig<DataConfiguration> dataConfiguration) {
    return new ChangeBehaviourWithDatabaseConfig(dataConfiguration);
  }


  @Provides
  FSConfig<DataConfiguration> dataConfiguration() {
    return new FSConfig<>(DataConfiguration.class, "data-configuration");
  }

  @Provides
  DBConfig<DataConfiguration> dataConfiguration(RepositoryHandler repositoryHandler) {
    return new DBConfig<>(DataConfiguration.class, repositoryHandler);
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
