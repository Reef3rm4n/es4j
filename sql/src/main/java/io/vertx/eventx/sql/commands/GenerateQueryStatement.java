package io.vertx.eventx.sql.commands;


import io.vertx.eventx.sql.QueryStatementType;
import io.vertx.eventx.sql.generator.filters.QueryFilters;
import io.vertx.eventx.sql.models.QueryOptions;

public record GenerateQueryStatement(
    String table,
    QueryStatementType type,
    QueryFilters queryBuilder,
    QueryOptions queryOptions
) {
}
