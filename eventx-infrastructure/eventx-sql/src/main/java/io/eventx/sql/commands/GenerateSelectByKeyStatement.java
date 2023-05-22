package io.eventx.sql.commands;

import java.util.Set;

public record GenerateSelectByKeyStatement(String table, Set<String> keyColumns) {
}
