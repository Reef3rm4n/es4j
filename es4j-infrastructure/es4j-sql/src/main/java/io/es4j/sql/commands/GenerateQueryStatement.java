package io.es4j.sql.commands;


import io.es4j.sql.generator.filters.QueryFilters;

import io.es4j.sql.models.QueryOptions;
import io.es4j.sql.models.QueryStatementType;

public record GenerateQueryStatement(
    String table,
    QueryStatementType type,
    QueryFilters queryBuilder,
    QueryOptions queryOptions
) {
}
