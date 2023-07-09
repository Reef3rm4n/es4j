package io.es4j.config;

import io.es4j.core.objects.DefaultFilters;

import java.util.List;

public record ConfigurationFilter(
  String tenant,
  List<String> configs,
  DefaultFilters options
) {
}
