package io.es4j.saga;

import io.smallrye.mutiny.Uni;


public interface SagaStore {


  Uni<SagaRecord> fetchSaga(String id);
  Uni<Void> update(SagaRecord command);

}
