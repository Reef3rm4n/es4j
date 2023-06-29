package io.es4j.domain;

import io.es4j.config.DatabaseConfiguration;

import java.util.Map;

public record DataBusinessRule(
  Boolean rule,
  String description,
  Map<String, Object> data

) implements DatabaseConfiguration {

}
