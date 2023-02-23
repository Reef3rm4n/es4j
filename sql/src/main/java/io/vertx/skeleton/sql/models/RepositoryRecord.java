package io.vertx.skeleton.sql.models;

import io.vertx.core.shareddata.Shareable;

public interface RepositoryRecord<V> extends Shareable {
    BaseRecord baseRecord();
    V with(BaseRecord baseRecord);
    default Boolean validate() {
        return true;
    }

}
