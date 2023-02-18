package io.vertx.skeleton.sql;

import io.smallrye.mutiny.tuples.Tuple;
import io.vertx.mutiny.core.shareddata.Lock;
import io.vertx.skeleton.sql.models.Query;
import io.vertx.skeleton.sql.models.RepositoryRecord;

import java.util.function.Consumer;

public class RecordStream<V extends RepositoryRecord<V>,Q extends Query> {

    private Lock lock;
    private Consumer<V> consumer;
    private String statement;
    private Tuple arguments;
    private Integer batchSize;
    private Q query;

    public Lock lock() {
        return lock;
    }

    public RecordStream<V, Q> setLock(Lock lock) {
        this.lock = lock;
        return this;
    }

    public Consumer<V> consumer() {
        return consumer;
    }

    public RecordStream<V, Q> setConsumer(Consumer<V> consumer) {
        this.consumer = consumer;
        return this;
    }

    public String statement() {
        return statement;
    }

    public RecordStream<V, Q> setStatement(String statement) {
        this.statement = statement;
        return this;
    }

    public Tuple arguments() {
        return arguments;
    }

    public RecordStream<V, Q> setArguments(Tuple arguments) {
        this.arguments = arguments;
        return this;
    }

    public Integer batchSize() {
        return batchSize;
    }

    public RecordStream<V, Q> setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Q query() {
        return query;
    }

    public RecordStream<V, Q> setQuery(Q query) {
        this.query = query;
        return this;
    }
}
