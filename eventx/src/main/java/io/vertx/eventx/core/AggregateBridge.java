package io.vertx.eventx.core;


import io.activej.inject.Injector;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.eventx.infrastructure.Bridge;
import io.vertx.eventx.infrastructure.misc.CustomClassLoader;
import io.vertx.mutiny.core.Vertx;

import java.util.List;

import static io.vertx.eventx.launcher.EventxMain.*;

public class AggregateBridge extends AbstractVerticle {

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateBridge.class);
  private final ModuleBuilder moduleBuilder;
  private List<Bridge> bridges;

  public AggregateBridge(ModuleBuilder moduleBuilder) {
    this.moduleBuilder = moduleBuilder;
  }

  private Injector startInjector() {
    moduleBuilder.bind(Vertx.class).toInstance(vertx);
    moduleBuilder.bind(JsonObject.class).toInstance(config());
    return Injector.of(moduleBuilder.build());
  }

  @Override
  public Uni<Void> asyncStart() {
    final var injector = startInjector();
    this.bridges = CustomClassLoader.loadFromInjector(injector, Bridge.class);
    LOGGER.info("Starting bridges {}", bridges);
    return Multi.createFrom().iterable(bridges)
      .onItem().transformToUniAndMerge(bridge -> bridge.start(vertx, config(), AGGREGATE_CLASSES))
      .collect().asList()
      .replaceWithVoid();
  }


  @Override
  public Uni<Void> asyncStop() {
    return Multi.createFrom().iterable(bridges)
      .onItem().transformToUniAndMerge(Bridge::close)
      .collect().asList()
      .replaceWithVoid();
  }

}
