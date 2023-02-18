package io.vertx.skeleton.sql.models;

public record EmptyQuery() implements Query {
    @Override
    public QueryOptions options() {
        return null;
    }
}
