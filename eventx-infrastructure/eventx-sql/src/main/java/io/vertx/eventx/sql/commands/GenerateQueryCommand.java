package io.vertx.eventx.sql.commands;


import io.vertx.eventx.sql.QueryStatementType;
import io.vertx.eventx.sql.models.Query;

public record GenerateQueryCommand<T extends Query>(
    QueryStatementType type,
    T query
) {
}
