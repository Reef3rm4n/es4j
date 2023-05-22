package io.eventx.test.domain;

import io.eventx.config.ConfigurationEntry;

import java.util.Map;

public record DataConfiguration(
  Boolean rule,
  String description,
  Map<String, Object> data

) implements ConfigurationEntry {

  @Override
  public String name() {
    return "data-configuration";
  }

}