package io.vertx.skeleton.sql;

import io.vertx.mutiny.core.Vertx;

public record JdbcUrl(String schema, String userName, String password, String jdbcUrl, Vertx vertx) {
}
