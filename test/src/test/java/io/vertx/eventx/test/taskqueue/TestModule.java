package io.vertx.eventx.test.taskqueue;

import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.vertx.eventx.common.VertxComponent;

public class TestModule extends VertxComponent {

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
