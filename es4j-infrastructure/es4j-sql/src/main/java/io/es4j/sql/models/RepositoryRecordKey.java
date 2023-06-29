package io.es4j.sql.models;

import io.vertx.core.shareddata.Shareable;

public interface RepositoryRecordKey extends Shareable {

    default String tenantId() {
        return "default";
    }
}
