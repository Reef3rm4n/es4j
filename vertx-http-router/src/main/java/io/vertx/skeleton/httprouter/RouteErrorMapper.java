package io.vertx.skeleton.httprouter;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.Tenant;

public interface RouteErrorMapper {


  Error map(Throwable throwable);

  Tenant tenant();
}
