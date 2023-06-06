package io.eventx.config.orm;



import io.soabase.recordbuilder.core.RecordBuilder;
import io.eventx.sql.models.Query;
import io.eventx.sql.models.QueryOptions;

import java.util.List;


@RecordBuilder
public record ConfigurationQuery(
  List<String> tClasses,
  QueryOptions options

) implements Query {


}
