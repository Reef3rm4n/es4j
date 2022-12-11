package io.vertx.skeleton.ccp.models;

import io.vertx.skeleton.models.Tenant;

import java.util.Objects;

public record QueueEvent<T, R>(
  String id,
  Tenant tenant,
  T entry,
  R result,
  Throwable throwable,
  QueueEventType queueEventType,
  Integer eventVersion
) {
  public static <T, R> QueueEvent<T, R> of(
    final String id,
    final Tenant tenant,
    final T entry,
    final R result,
    final Throwable throwable,
    final QueueEventType queueEventType,
    final Integer eventVersion
  ) {
    return new QueueEvent<>(id, tenant, entry, result, throwable, queueEventType, Objects.requireNonNullElse(eventVersion,0));
  }
}
