package io.vertx.eventx.sql.commands;

import java.util.Set;

public record GenerateInsertStatement(String table, Set<String> columns) {
}
