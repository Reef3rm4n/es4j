package io.vertx.skeleton.stepreactor;

import io.smallrye.mutiny.Uni;

public interface Step<T extends ReactorRequest>  {

  Uni<T> process(T dataModel);
  default Uni<Void> rollback(T dataModel) {
    return Uni.createFrom().voidItem();
  }

  default Uni<Boolean> canSkip(T dataModel) {
    return Uni.createFrom().item(false);
  }

}
