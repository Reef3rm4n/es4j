package io.eventx.sql.commands;


import io.eventx.sql.models.QueryStatementType;
import io.eventx.sql.models.Query;

public record GenerateQueryCommand<T extends Query>(
    QueryStatementType type,
    T query
) {
}
