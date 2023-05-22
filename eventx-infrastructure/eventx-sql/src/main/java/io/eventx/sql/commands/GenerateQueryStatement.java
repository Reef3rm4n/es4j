package io.eventx.sql.commands;


import io.eventx.sql.generator.filters.QueryFilters;

import io.eventx.sql.models.QueryOptions;
import io.eventx.sql.models.QueryStatementType;

public record GenerateQueryStatement(
    String table,
    QueryStatementType type,
    QueryFilters queryBuilder,
    QueryOptions queryOptions
) {
}
