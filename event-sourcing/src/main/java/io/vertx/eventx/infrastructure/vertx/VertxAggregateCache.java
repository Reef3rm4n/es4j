package io.vertx.eventx.infrastructure.vertx;

import io.vertx.eventx.Aggregate;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.shareddata.Shareable;
import io.vertx.eventx.objects.EntityState;
import io.vertx.eventx.infrastructure.pg.models.AggregateRecordKey;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.shareddata.LocalMap;

import java.util.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

// todo move this to caffeine, which is probably better suited for this kind of stuff.
public class VertxAggregateCache<T extends Aggregate, V extends EntityState<T>> {

  private final Vertx vertx;
  private final Long aggregateTtlInMinutes;
  private static final Logger LOGGER = LoggerFactory.getLogger(VertxAggregateCache.class);
  private final Class<T> aggregateClass;

  public VertxAggregateCache(
    Vertx vertx,
    Class<T> aggregateClass,
    Long aggregateTtlInMinutes
  ) {
    this.vertx = vertx;
    this.aggregateClass = aggregateClass;
    this.aggregateTtlInMinutes = aggregateTtlInMinutes;
  }

  public V get(AggregateRecordKey k) {
    final var holder = localEntityMap().get(k);
    if (holder != null && holder.hasNotExpired()) {
      return holder.value;
    } else {
      return null;
    }
  }

  public Uni<V> put(AggregateRecordKey k, V v) {
    long timestamp = System.nanoTime();
    long timerId = vertx.setTimer(aggregateTtlInMinutes, l -> removeIfExpired(k));
    Holder<V> previous = localEntityMap().put(k, new Holder<>(v, timerId, aggregateTtlInMinutes * 60000, timestamp));
    if (previous != null) {
      vertx.cancelTimer(previous.timerId);
    }
    return Uni.createFrom().item(v);
  }

  public Integer size() {
    return localEntityMap().size();
  }

  public List<Holder<V>> values() {
    return vertx.getDelegate().sharedData().<AggregateRecordKey, Holder<V>>getLocalMap(aggregateClass.getName())
      .values().stream()
      .toList();
  }

  private LocalMap<AggregateRecordKey, Holder<V>> localEntityMap() {
    return vertx.sharedData().<AggregateRecordKey, Holder<V>>getLocalMap(aggregateClass.getName());
  }

  private void removeIfExpired(AggregateRecordKey k) {
    final var value = localEntityMap().get(k);
    if (!value.hasNotExpired()) {
      LOGGER.info(k + " evicted form cache");
      remove(k);
    }
  }

  public V remove(AggregateRecordKey k) {
    final var previous = localEntityMap().remove(k);
    if (previous != null) {
      if (previous.expires()) {
        vertx.cancelTimer(previous.timerId);
      }
      return previous.value;
    } else {
      return null;
    }
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
      return !expires() || MILLISECONDS.convert(System.nanoTime() - timestamp, NANOSECONDS) < ttl;
    }

    public String toString() {
      return "Holder{" + "value=" + value + ", verticleId=" + timerId + ", ttl=" + ttl + ", timestamp=" + timestamp + '}';
    }
  }
}
