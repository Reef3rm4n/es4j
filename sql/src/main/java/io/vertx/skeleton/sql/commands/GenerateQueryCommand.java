package io.vertx.skeleton.sql.commands;


import io.vertx.skeleton.sql.QueryStatementType;
import io.vertx.skeleton.sql.models.Query;

public record GenerateQueryCommand<T extends Query>(
    QueryStatementType type,
    T query
) {
}
