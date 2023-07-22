package io.es4j.infrastructure.models;

import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.util.List;

public record AvailableAggregate(
  String aggregate,
  List<String> tenants
) implements Serializable, Shareable {
}
