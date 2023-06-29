package io.es4j.sql.commands;

import java.util.Set;

public record GenerateInsertStatement(String table, Set<String> columns) {
}
