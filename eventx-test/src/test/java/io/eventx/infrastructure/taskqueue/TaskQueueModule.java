package io.eventx.infrastructure.taskqueue;

import com.google.auto.service.AutoService;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.eventx.core.objects.EventxModule;

@AutoService(EventxModule.class)
public class TaskQueueModule extends EventxModule {

  @Provides
  @Inject
  MockDeadPayloadProcessor mockDeadPayloadProcessor() {
    return new MockDeadPayloadProcessor();
  }

  @Provides
  @Inject
  MockProcessor mockProcessor() {
    return new MockProcessor();
  }

}
