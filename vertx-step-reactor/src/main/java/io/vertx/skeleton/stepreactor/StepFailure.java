package io.vertx.skeleton.stepreactor;

public record StepFailure<T extends ReactorRequest>(
  Integer order,
  Class<Step<T>> stepClass,
  Throwable throwable
) {
}
