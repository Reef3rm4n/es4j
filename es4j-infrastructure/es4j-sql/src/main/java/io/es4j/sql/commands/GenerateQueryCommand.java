package io.es4j.sql.commands;


import io.es4j.sql.models.QueryStatementType;
import io.es4j.sql.models.Query;

public record GenerateQueryCommand<T extends Query>(
    QueryStatementType type,
    T query
) {
}
