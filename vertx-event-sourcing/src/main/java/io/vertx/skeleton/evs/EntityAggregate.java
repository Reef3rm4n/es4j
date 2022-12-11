package io.vertx.skeleton.evs;

import io.vertx.skeleton.models.Tenant;
import io.vertx.core.shareddata.Shareable;

public interface EntityAggregate extends Shareable {

  String entityId();

  Tenant tenant();

}
