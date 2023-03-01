package io.vertx.eventx.sql.commands;

import java.util.Set;

public record GenerateUpdateByKeyStatement(String table, Set<String> keyColumns, Set<String> updateAbleColumns) {
}
