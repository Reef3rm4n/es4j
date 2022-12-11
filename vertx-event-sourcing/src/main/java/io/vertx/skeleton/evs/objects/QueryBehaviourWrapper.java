package io.vertx.skeleton.evs.objects;

import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.EntityAggregateQuery;
import io.vertx.skeleton.evs.QueryBehaviour;

public  record  QueryBehaviourWrapper<T extends EntityAggregate, Q extends EntityAggregateQuery>(
  QueryBehaviour<T, Q> delegate,
  Class<T> entityAggregateClass,
  Class<Q> queryClass
) {
}
