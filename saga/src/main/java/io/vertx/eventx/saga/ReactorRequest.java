package io.vertx.eventx.saga;

import io.vertx.eventx.common.CommandHeaders;

public interface ReactorRequest {

  CommandHeaders commandHeaders();

}
