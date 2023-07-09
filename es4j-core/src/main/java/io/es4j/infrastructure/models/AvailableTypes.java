package io.es4j.infrastructure.models;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

public record AvailableTypes(
  List<String> events,
  Map<String, JsonNode> commandSchemas
) {
}
