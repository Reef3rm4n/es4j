package io.vertx.eventx.sql.commands;

import java.util.Set;

public record GenerateDeleteByKeyStatement(String table, Set<String> keyColumns) {
}
