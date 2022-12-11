package io.vertx.skeleton.evs;

import io.vertx.skeleton.evs.objects.ConsistencyStrategy;
import io.vertx.skeleton.models.Tenant;

public interface QueryBehaviour<T extends EntityAggregate, Q extends EntityAggregateQuery> {

  Object query(T state, Q query);

  default ConsistencyStrategy strategy() {
    return ConsistencyStrategy.EVENTUALLY_CONSISTENT;
  }

  default Tenant tenant() {
    return null;
  }



}
