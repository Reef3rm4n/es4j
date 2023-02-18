package io.vertx.skeleton.sql.commands;

import java.util.Set;

public record GenerateDeleteByKeyStatement(String table, Set<String> keyColumns) {
}
