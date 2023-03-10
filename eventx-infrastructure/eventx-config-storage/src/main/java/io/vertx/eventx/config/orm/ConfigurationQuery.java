package io.vertx.eventx.config.orm;



import io.vertx.eventx.sql.models.Query;
import io.vertx.eventx.sql.models.QueryOptions;

import java.util.List;

public record ConfigurationQuery(
  List<String> name,
  List<String> tClasses,
  QueryOptions options

) implements Query {


}
