package io.es4j.infrastructure.models;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;
import java.util.Set;

public record AvailableTypes(
  Set<String> events,
  Map<String, JsonNode> commandSchemas
) {
}
