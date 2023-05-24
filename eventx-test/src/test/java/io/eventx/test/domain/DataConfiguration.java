package io.eventx.test.domain;

import io.eventx.config.Configuration;

import java.util.Map;

public record DataConfiguration(
  Boolean rule,
  String description,
  Map<String, Object> data

) implements Configuration {

  @Override
  public String name() {
    return "data-configuration";
  }

}
