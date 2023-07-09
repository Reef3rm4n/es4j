package io.es4j.infra.pg.models;


import io.es4j.sql.models.Query;
import io.es4j.sql.models.QueryOptions;

import java.util.List;

public record EventJournalOffsetFilter(
  List<String> consumers,
  QueryOptions options
) implements Query {
}
