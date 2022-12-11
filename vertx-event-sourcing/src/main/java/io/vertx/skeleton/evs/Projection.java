package io.vertx.skeleton.evs;

import io.vertx.skeleton.evs.objects.ConsistencyStrategy;
import io.vertx.skeleton.models.Tenant;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.SqlConnection;

import java.util.*;

public interface Projection<T extends EntityAggregate> {


  Uni<Void> applyEvents(T entityAggregateState, List<Object> events, SqlConnection sqlConnection);

  default List<Class<?>> eventsClasses() {
    return null;
  }

  default ConsistencyStrategy strategy() {
    return ConsistencyStrategy.EVENTUALLY_CONSISTENT;
  }

  default Tenant tenant() {
    return null;
  }

}
