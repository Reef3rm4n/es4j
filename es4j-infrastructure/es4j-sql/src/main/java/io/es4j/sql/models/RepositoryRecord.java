package io.es4j.sql.models;

import io.vertx.core.shareddata.Shareable;

public interface RepositoryRecord<V> extends Shareable {
    BaseRecord baseRecord();
    V with(BaseRecord baseRecord);
    default Boolean validate() {
        return true;
    }

}
