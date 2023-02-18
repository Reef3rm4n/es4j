package io.vertx.skeleton.sql.models;

import io.vertx.core.shareddata.Shareable;

public interface RepositoryRecord<V> extends Shareable {
    RecordWithoutID baseRecord();
    V with(RecordWithoutID baseRecord);
    default Boolean validate() {
        return true;
    }

}
