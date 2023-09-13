package io.es4j.saga;


import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class SagaOrchestrator {

  List<SagaManager> sagas;
  SagaStore sagaStore;



  public Uni<Void> route(String className, JsonObject request) {
    final var sagaWrapper = sagas.stream().filter(saga -> saga.isMatch(className)).findFirst().orElseThrow(() -> new IllegalArgumentException("Saga not found"));
    final var payload = sagaWrapper.supplyPayload(request);

    return null;
  }
}
