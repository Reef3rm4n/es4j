package io.es4j.sql.models;

public record EmptyQuery() implements Query {
    @Override
    public QueryOptions options() {
        return null;
    }
}
