package io.vertx.eventx.http;

import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.vertx.eventx.objects.EventxModule;
import io.vertx.mutiny.core.Vertx;

import java.util.List;

public class HttpBridgeModule extends EventxModule {


  @Provides
  @Inject
  HttpBridge httpBridge(
    final Vertx vertx,
    final List<HttpRoute> routes,
    final List<HealthCheck> healthChecks
  ) {
    return new HttpBridge(vertx, routes, healthChecks);
  }

  @Provides
  @Inject
  List<HttpRoute> httpRoutes(Injector injector) {
    return io.vertx.eventx.common.CustomClassLoader.loadFromInjector(injector, HttpRoute.class);
  }

  @Provides
  @Inject
  List<HealthCheck> healthChecks(Injector injector) {
    return io.vertx.eventx.common.CustomClassLoader.loadFromInjector(injector, HealthCheck.class);
  }

}
