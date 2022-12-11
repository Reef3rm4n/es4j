package io.vertx.skeleton.stepreactor;

import java.util.List;

public record ProcessStateData<T extends ReactorRequest>(
  ProcessState processState,
  List<Class<Step<T>>> executedSteps,
  List<StepFailure<T>> failures
) {

}
