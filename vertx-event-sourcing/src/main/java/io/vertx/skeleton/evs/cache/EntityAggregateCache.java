package io.vertx.skeleton.evs.cache;

import io.vertx.skeleton.evs.objects.EntityAggregateState;
import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.models.EntityAggregateKey;
import io.smallrye.mutiny.Uni;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Shareable;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.shareddata.LocalMap;

import java.util.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toMap;

public class EntityAggregateCache<T extends EntityAggregate, V extends EntityAggregateState<T>> {

  private final Vertx vertx;
  private final Long aggregateTtlInMinutes;
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityAggregateCache.class);
  private final Class<T> aggregateClass;
  private final String handlerAddress;
  private long refreshTaskTimerId;

  public EntityAggregateCache(
    Vertx vertx,
    Class<T> aggregateClass,
    String handlerAddress,
    Long aggregateTtlInMinutes
  ) {
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
    this.aggregateTtlInMinutes = aggregateTtlInMinutes;
    this.handlerAddress = handlerAddress;
  }

  public void registerHandler() {
    LOGGER.info("Publishing handler registration -> " + handlerAddress);
    vertx.eventBus().publish(AddressResolver.localAvailableHandlers(aggregateClass), handlerAddress, new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.ADD.name()));
    handlerRefreshTimer(aggregateTtlInMinutes);
  }

  private void handlerRefreshTimer(Long delay) {
    this.refreshTaskTimerId = vertx.setTimer(
      delay,
      d -> {
        vertx.eventBus().publish(AddressResolver.localAvailableHandlers(aggregateClass), handlerAddress, new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.ADD.name()));
        LOGGER.info("Published handler keep-alive -> " + handlerAddress);
        handlerRefreshTimer(delay * 60000);
      }
    );
  }

  public void unregisterHandler() {
    LOGGER.info("Publishing handler removal -> " + handlerAddress);
    vertx.eventBus().publish(AddressResolver.localAvailableHandlers(aggregateClass), handlerAddress, new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.REMOVE.name()));
    vertx.cancelTimer(refreshTaskTimerId);
  }

  public V get(EntityAggregateKey k) {
    final var holder = localEntityMap().get(k);
    if (holder != null && holder.hasNotExpired()) {
      return holder.value;
    } else {
      return null;
    }
  }

  public Uni<V> put(EntityAggregateKey k, V v) {
    long timestamp = System.nanoTime();
    long timerId = vertx.setTimer(aggregateTtlInMinutes, l -> removeIfExpired(k));
    Holder<V> previous = localEntityMap().put(k, new Holder<>(v, timerId, aggregateTtlInMinutes * 60000, timestamp));
    LOGGER.info("EntityAggregate added to handler cache -> " + k + " handler address -> " + handlerAddress);
    if (previous != null) {
      vertx.cancelTimer(previous.timerId);
    } else {
      publishEntityHandlerAddress(k);
    }
    return Uni.createFrom().item(v);
  }

  public Integer size() {
    return localEntityMap().size();
  }

  public List<Holder<V>> values() {
    return vertx.getDelegate().sharedData().<EntityAggregateKey, Holder<V>>getLocalMap(aggregateClass.getName())
      .values().stream()
      .toList();
  }

  private LocalMap<EntityAggregateKey, Holder<V>> localEntityMap() {
    return vertx.sharedData().<EntityAggregateKey, Holder<V>>getLocalMap(aggregateClass.getName());
  }

  private void publishEntityHandlerAddress(EntityAggregateKey k) {
    LOGGER.info("Publishing " + k + " address -> " + handlerAddress);
    vertx.eventBus().publish(
      AddressResolver.localAggregateHandler(aggregateClass),
      JsonObject.mapFrom(new EntityAggregateHandlerAddress(k, handlerAddress)),
      new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.ADD.name())
    );
  }

  private void removeIfExpired(EntityAggregateKey k) {
    final var value = localEntityMap().get(k);
    if (! value.hasNotExpired()) {
      LOGGER.info(k + " evicted form cache");
      localEntityMap().remove(k);
      publishEntityAggregateRemoval(k);
    }
  }

  private void publishEntityAggregateRemoval(EntityAggregateKey k) {
    vertx.eventBus().publish(
      AddressResolver.localAggregateHandler(aggregateClass),
      JsonObject.mapFrom(new EntityAggregateHandlerAddress(k, handlerAddress)),
      new DeliveryOptions().addHeader(Actions.ACTION.name(), Actions.REMOVE.name())
    );
    LOGGER.info("EntityAggregate removal published -> " + k);
  }

  public Uni<V> remove(EntityAggregateKey k) {
    final var previous = localEntityMap().remove(k);
    if (previous != null) {
      if (previous.expires()) {
        vertx.cancelTimer(previous.timerId);
      }
      return Uni.createFrom().item(previous.value);
    } else {
      return Uni.createFrom().item(null);
    }
  }

  public Map<EntityAggregateKey, Holder<V>> entries() {
    return vertx.getDelegate().sharedData().<EntityAggregateKey, Holder<V>>getLocalMap(aggregateClass.getName())
      .entrySet().stream()
      .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static class Holder<V> implements Shareable {
    final V value;
    final long timerId;
    final long ttl;
    final long timestamp;

    Holder(V value) {
      Objects.requireNonNull(value);
      this.value = value;
      timestamp = ttl = timerId = 0;
    }

    Holder(V value, long timerId, long ttl, long timestamp) {
      Objects.requireNonNull(value);
      if (ttl < 1) {
        throw new IllegalArgumentException("ttl must be positive: " + ttl);
      }
      this.value = value;
      this.timerId = timerId;
      this.ttl = ttl;
      this.timestamp = timestamp;
    }

    boolean expires() {
      return ttl > 0;
    }

    boolean hasNotExpired() {
      return ! expires() || MILLISECONDS.convert(System.nanoTime() - timestamp, NANOSECONDS) < ttl;
    }

    public String toString() {
      return "Holder{" + "value=" + value + ", verticleId=" + timerId + ", ttl=" + ttl + ", timestamp=" + timestamp + '}';
    }
  }
}
