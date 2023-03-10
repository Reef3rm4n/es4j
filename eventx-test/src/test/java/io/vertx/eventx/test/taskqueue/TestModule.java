package io.vertx.eventx.test.taskqueue;

import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;

public class TestModule extends EventxModule {

  @Inject
  @Provides
  MockProcessor mockProcessor() {
    return new MockProcessor();
  }

  @Inject
  @Provides
  MockDeadPayloadProcessor mockDeadProcessor() {
    return new MockDeadPayloadProcessor();
  }


}
