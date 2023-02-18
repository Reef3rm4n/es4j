

package io.vertx.skeleton.sql.models;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.shareddata.Lock;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.skeleton.sql.exceptions.OrmGenericException;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public interface RecordRepository<K, V extends RepositoryRecord<V>, Q extends Query> {

    default Uni<V> selectUnique(String statement) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> selectUnique(String statement, Map<String, Object> map) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> selectUnique(String statement, Map<String, Object> map, SqlConnection sqlConnection) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> selectByKey(K key) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> selectByKey(K key, SqlConnection sqlConnection) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<List<V>> query(Q query) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<List<V>> query(Q query, SqlConnection sqlConnection) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<List<V>> query(String query) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<List<V>> query(String query, SqlConnection sqlConnection) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<List<V>> query(String statement, Map<String, Object> params) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<Void> exists(K key) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<Void> exists(K key, String statement) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> insert(V value) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> insert(V value, SqlConnection connection) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> updateByKey(V value, SqlConnection connection) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<Void> updateByKeyBatch(List<V> value, SqlConnection sqlConnection) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<Void> updateBatch(String query, List<Map<String, Object>> params, SqlConnection sqlConnection) {
        throw OrmGenericException.notImplemented();
    }

    Uni<Void> updateBatch(String query, List<Map<String, Object>> params);

    Uni<Void> update(String query, Map<String, Object> params, SqlConnection sqlConnection);

    Uni<Void> update(String query, Map<String, Object> params);

    default Uni<Void> updateByKeyBatch(List<V> value) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<Void> insertBatch(List<V> value) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<Void> insertBatch(List<V> value, SqlConnection sqlConnection) {
        throw OrmGenericException.notImplemented();
    }

    default void insertAndForget(V value) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> updateByKey(V value) {
        throw OrmGenericException.notImplemented();
    }

    default void updateAndForget(V value) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> updateByKey(V value, String statement) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<Void> deleteByKey(K key) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<Void> deleteByKey(K key, SqlConnection sqlConnection) {
        throw OrmGenericException.notImplemented();
    }

    Uni<Void> deleteByKeyBatch(List<K> keys, SqlConnection sqlConnection);

    default Uni<Integer> count(Q query) {
        throw OrmGenericException.notImplemented();
    }


    Uni<Integer> count(String query, Map<String, Object> params);

    default Uni<Void> deleteQuery(Q query, SqlConnection sqlConnection) {
        throw OrmGenericException.notImplemented();
    }

    Uni<Void> deleteQuery(Q query);

    default void deleteAndForget(K key) {
        throw OrmGenericException.notImplemented();
    }

    default void deleteAndForget(String statement, Map<String, Object> params) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> deleteUnique(String statement) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> deleteUnique(String statement, Map<String, Object> map) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<V> deleteUnique(String statement, Map<String, Object> map, SqlConnection sqlConnection) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<List<V>> query(String statement, Map<String, Object> params, SqlConnection sqlConnection) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<Void> stream(Uni<Void> lock, Consumer<V> handler, String statement, Tuple arguments) {
        throw OrmGenericException.notImplemented();
    }

    Uni<Void> stream(Uni<Void> lock, Consumer<V> handler, Q query);


    default Uni<Void> stream(Lock lock, Consumer<V> handler, String statement) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<Void> stream(Lock lock, Consumer<V> handler, String statement, Tuple arguments) {
        throw OrmGenericException.notImplemented();
    }

    default Uni<Void> stream(Lock lock, Consumer<V> handler, String statement, Tuple arguments, Integer batchSize) {
        throw OrmGenericException.notImplemented();
    }

    default <T> Uni<T> transaction(Function<SqlConnection, Uni<T>> function) {
        throw OrmGenericException.notImplemented();
    }
}
