package io.vertx.skeleton.sql.commands;


import io.vertx.skeleton.sql.QueryStatementType;
import io.vertx.skeleton.sql.generator.filters.QueryFilters;
import io.vertx.skeleton.sql.models.QueryOptions;

public record GenerateQueryStatement(
    String table,
    QueryStatementType type,
    QueryFilters queryBuilder,
    QueryOptions queryOptions
) {
}
