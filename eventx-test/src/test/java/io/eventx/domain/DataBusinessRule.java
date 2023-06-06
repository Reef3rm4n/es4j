package io.eventx.domain;

import io.eventx.config.BusinessRule;

import java.util.Map;

public record DataBusinessRule(
  Boolean rule,
  String description,
  Map<String, Object> data

) implements BusinessRule {

}
