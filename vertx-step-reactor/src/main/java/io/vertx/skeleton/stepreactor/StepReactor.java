package io.vertx.skeleton.stepreactor;


import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class StepReactor<T extends ReactorRequest> {

  private List<Class<Step<T>>> order;
  private List<Step<T>> steps;


  public Uni<Void> process(T reactorRequest, ProcessStateData<T> processState) {
    final var atomicReference = new AtomicReference<>(reactorRequest);
    final var executedSteps = new ArrayList<Class<?>>();
    return Multi.createFrom().iterable(steps)
      .onItem().transformToUniAndConcatenate(
        step -> step.process(atomicReference.get())
          .map(state -> {
              atomicReference.set(state);
              return executedSteps.add(step.getClass());
            }
          )
      ).collect().asList()
      .replaceWithVoid();
  }


}
