package io.eventx.http;

import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.eventx.config.orm.ConfigurationRecordMapper;
import io.eventx.core.objects.EventxModule;
import io.eventx.infrastructure.Bridge;
import io.eventx.infrastructure.misc.CustomClassLoader;
import io.eventx.sql.Repository;
import io.eventx.sql.RepositoryHandler;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.List;

public class HttpBridgeModule extends EventxModule {

  @Provides
  @Inject
  Bridge bridge(OptionalDependency<CommandAuth> commandAuth, final List<HttpRoute> routes, final List<HealthCheck> healthChecks) {
    return new HttpBridge(commandAuth, routes, healthChecks);
  }

  @Provides
  @Inject
  List<HttpRoute> httpRoutes(Injector injector) {
    return CustomClassLoader.loadFromInjector(injector, HttpRoute.class);
  }

  @Provides
  @Inject
  List<HealthCheck> healthChecks(Injector injector) {
    return CustomClassLoader.loadFromInjector(injector, HealthCheck.class);
  }

  @Provides
  @Inject
  OptionalDependency<CommandAuth> commandAuth(Injector injector) {
    if (CustomClassLoader.checkPresence(injector, CommandAuth.class)) {
      return OptionalDependency.of(CustomClassLoader.loadFromInjectorClass(injector, CommandAuth.class)
        .stream().findFirst().orElseThrow());
    }
    return OptionalDependency.empty();
  }


  @Provides
  @Inject
  @Named("configuration-route")
  HttpRoute configurationRoute(JsonObject configuraiton, Vertx vertx) {
    return new ConfigurationRoute(new Repository<>(ConfigurationRecordMapper.INSTANCE, RepositoryHandler.leasePool(configuraiton, vertx)));
  }


}
