package io.eventx.core.verticles;


import io.eventx.infrastructure.misc.Loader;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.crac.Context;
import org.crac.Core;
import org.crac.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.eventx.infrastructure.Bridge;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class AggregateBridge extends AbstractVerticle implements Resource {

  private final List<Bridge> bridges;

  @Override
  public void beforeCheckpoint(Context<? extends Resource> context) throws Exception {
    Promise<Void> p = Promise.promise();
    stop(p);
    CountDownLatch latch = new CountDownLatch(1);
    p.future().onComplete(event -> latch.countDown());
    latch.await();
  }

  @Override
  public void afterRestore(Context<? extends Resource> context) throws Exception {
    Promise<Void> p = Promise.promise();
    start(p);
    CountDownLatch latch = new CountDownLatch(1);
    p.future().onComplete(event -> latch.countDown());
    latch.await();
  }

  protected static final Logger LOGGER = LoggerFactory.getLogger(AggregateBridge.class);

  public AggregateBridge() {
    this.bridges = Loader.loadBridges();
    Core.getGlobalContext().register(this);
  }

  @Override
  public Uni<Void> asyncStart() {
    LOGGER.info("Deploying bridges {}", bridges);
    return Multi.createFrom().iterable(bridges)
      .onItem().transformToUniAndMerge(bridge -> bridge.start(vertx, config()))
      .collect().asList()
      .replaceWithVoid();
  }


  @Override
  public Uni<Void> asyncStop() {
    return Multi.createFrom().iterable(bridges)
      .onItem().transformToUniAndMerge(Bridge::stop)
      .collect().asList()
      .replaceWithVoid();
  }

}
