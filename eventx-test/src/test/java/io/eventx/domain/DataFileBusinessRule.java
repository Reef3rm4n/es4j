package io.eventx.domain;


import java.util.Map;

public record DataFileBusinessRule(
  Boolean rule,
  String description,
  Map<String, Object> data

) {

}
