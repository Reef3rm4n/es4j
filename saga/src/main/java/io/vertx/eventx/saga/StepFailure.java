package io.vertx.eventx.saga;

public record StepFailure<T extends ReactorRequest>(
  Integer order,
  Class<Step<T>> stepClass,
  Throwable throwable
) {
}
