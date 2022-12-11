package io.vertx.skeleton.evs;


import io.vertx.skeleton.models.Tenant;
import io.smallrye.mutiny.Uni;

public interface CommandValidator<T extends EntityAggregate, C extends EntityAggregateCommand> {


  Uni<EntityAggregateCommand> validate(C command, T state);


  default Tenant tenant() {
    return null;
  }


}
