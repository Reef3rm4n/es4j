package io.eventx.core.verticles;


import io.activej.inject.Injector;
import io.activej.inject.module.ModuleBuilder;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import org.crac.Context;
import org.crac.Core;
import org.crac.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.eventx.infrastructure.Bridge;
import io.eventx.infrastructure.misc.CustomClassLoader;
import io.vertx.mutiny.core.Vertx;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static io.eventx.launcher.EventxMain.*;

public class AggregateBridge extends AbstractVerticle implements Resource {
  @Override
  public void beforeCheckpoint(Context<? extends Resource> context) throws Exception {
    final var countDownLatch = new CountDownLatch(1);

  }

  @Override
  public void afterRestore(Context<? extends Resource> context) throws Exception {

  }

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateBridge.class);
  private final ModuleBuilder moduleBuilder;
  private List<Bridge> bridges;

  public AggregateBridge(ModuleBuilder moduleBuilder) {
    this.moduleBuilder = moduleBuilder;
    Core.getGlobalContext().register(this);
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
