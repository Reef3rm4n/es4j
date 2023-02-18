package io.vertx.skeleton.sql.models;

public interface RepositoryRecordKey {

    default String tenant() {
        return "default";
    }
}
