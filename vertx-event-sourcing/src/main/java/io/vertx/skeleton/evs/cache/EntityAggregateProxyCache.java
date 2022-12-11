package io.vertx.skeleton.evs.cache;

import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.models.CacheException;
import io.vertx.skeleton.models.EntityAggregateKey;
import io.vertx.skeleton.models.Error;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.shareddata.Counter;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;
import org.ishugaliy.allgood.consistent.hash.node.ServerNode;

import java.util.*;

public class EntityAggregateProxyCache<T extends EntityAggregate> {

  private final Vertx vertx;
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityAggregateProxyCache.class);
  private final Class<T> aggregateClass;

  public EntityAggregateProxyCache(
    Vertx vertx,
    Class<T> aggregateClass
  ) {
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
  }

  public Uni<String> getHandler(EntityAggregateKey key) {
    final var address = localEntityAggregateHandlerMap().get(key);
    if (address == null) {
      LOGGER.warn("EntityAggregate not loaded into cache, will look for the most available handler -> " + key);
      return mostAvailableHandler();
    } else {
      LOGGER.debug("EntityAggregate " + key + " found in cache at address -> " + address);
      return Uni.createFrom().item(address);
    }
  }

  public Uni<String> mostAvailableHandler() {
    final var keys = localAvailableHandlersMap().keySet();
    if (keys.isEmpty()) {
      LOGGER.error("No handlers registered in the proxy cache");
      throw new CacheException(new Error("Handlers not available", "Handlers not available for type -> " + aggregateClass.getTypeName(), 400));
    }

    ServerNode n1 = new ServerNode("192.168.1.1", 80);
    ServerNode n2 = new ServerNode("192.168.1.132", 80);
    ServerNode n3 = new ServerNode("aws", "11.32.98.1", 9231);
    ServerNode n4 = new ServerNode("aws", "11.32.328.1", 9231);

// Build hash ring
    ConsistentHash<ServerNode> ring = HashRing.<ServerNode>newBuilder()
      .name("entityAggregateRing")       // set hash ring name
      .hasher(DefaultHasher.METRO_HASH)   // hash function to distribute partitions
      .partitionRate(10)                  // number of partitions per node
      .nodes(Arrays.asList(n1, n2))       // initial nodes set
      .build();

// add nodes
    ring.addAll(Arrays.asList(n3, n4));
// Locate 2 nodes
    Set<ServerNode> nodes = ring.locate("your_key", 2);

    return Multi.createFrom().iterable(keys)
      .onItem().transformToUniAndMerge(k -> localHandlerCounterMap(k)
        .flatMap(Counter::get)
        .map(size -> Tuple2.of(k, size))
      )
      .collect().asList()
      .map(counters -> counters.stream()
        .min(Comparator.comparing(Tuple2::getItem2))
        .map(Tuple2::getItem1)
        .orElseThrow(() -> new CacheException(new Error("Unable to calculate most available handler", "", 500)))
      )
      .invoke(address -> LOGGER.debug("Most available handler at address -> " + address));
  }


  private LocalMap<String, String> localAvailableHandlersMap() {
    return vertx.getDelegate().sharedData().<String, String>getLocalMap(AddressResolver.localAvailableHandlers(aggregateClass));
  }

  private io.vertx.mutiny.core.shareddata.LocalMap<EntityAggregateKey, String> localEntityAggregateHandlerMap() {
    return vertx.sharedData().<EntityAggregateKey, String>getLocalMap(AddressResolver.localAggregateHandler(aggregateClass));
  }

  private Uni<Counter> localHandlerCounterMap(final String handlerAddress) {
    return vertx.sharedData().getLocalCounter(AddressResolver.localAvailableHandlerCounter(aggregateClass, handlerAddress));
  }
}
