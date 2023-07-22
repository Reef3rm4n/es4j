package io.es4j.config.orm;



import io.soabase.recordbuilder.core.RecordBuilder;
import io.es4j.sql.models.Query;
import io.es4j.sql.models.QueryOptions;

import java.util.List;


@RecordBuilder
public record ConfigurationQuery(
  List<String> tClasses,
  QueryOptions options

) implements Query {


}
