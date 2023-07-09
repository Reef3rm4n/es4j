package io.es4j.infrastructure.models;

import io.es4j.core.objects.DefaultFilters;

import java.util.List;

public record OffsetFilter(
  List<String> consumers,
  String tenant,
  DefaultFilters options
  ) {
}
