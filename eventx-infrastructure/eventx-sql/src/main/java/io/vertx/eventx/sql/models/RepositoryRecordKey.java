package io.vertx.eventx.sql.models;

import io.vertx.core.shareddata.Shareable;

public interface RepositoryRecordKey extends Shareable {

    default String tenantId() {
        return "default";
    }
}
