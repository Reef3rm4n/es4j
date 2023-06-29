package io.es4j.sql.commands;

import java.util.Set;

public record GenerateUpdateByKeyStatement(String table, Set<String> keyColumns, Set<String> updateAbleColumns) {
}
