

package io.es4j.sql.models;

import io.es4j.sql.exceptions.GenericFailure;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.SqlConnection;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public interface RecordRepository<K, V extends RepositoryRecord<V>, Q extends Query> {

    default Uni<V> selectUnique(String statement) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> selectUnique(String statement, Map<String, Object> map) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> selectUnique(String statement, Map<String, Object> map, SqlConnection sqlConnection) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> selectByKey(K key) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> selectByKey(K key, SqlConnection sqlConnection) {
        throw GenericFailure.notImplemented();
    }

    default Uni<List<V>> query(Q query) {
        throw GenericFailure.notImplemented();
    }

    default Uni<List<V>> query(Q query, SqlConnection sqlConnection) {
        throw GenericFailure.notImplemented();
    }

    default Uni<List<V>> query(String query) {
        throw GenericFailure.notImplemented();
    }

    default Uni<List<V>> query(String query, SqlConnection sqlConnection) {
        throw GenericFailure.notImplemented();
    }

    default Uni<List<V>> query(String statement, Map<String, Object> params) {
        throw GenericFailure.notImplemented();
    }

    default Uni<Void> exists(K key) {
        throw GenericFailure.notImplemented();
    }

    default Uni<Void> exists(K key, String statement) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> insert(V value) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> insert(V value, SqlConnection connection) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> updateByKey(V value, SqlConnection connection) {
        throw GenericFailure.notImplemented();
    }

    default Uni<Void> updateByKeyBatch(List<V> value, SqlConnection sqlConnection) {
        throw GenericFailure.notImplemented();
    }

    default Uni<Void> updateBatch(String query, List<Map<String, Object>> params, SqlConnection sqlConnection) {
        throw GenericFailure.notImplemented();
    }

    Uni<Void> updateBatch(String query, List<Map<String, Object>> params);

    Uni<Void> update(String query, Map<String, Object> params, SqlConnection sqlConnection);

    Uni<Void> update(String query, Map<String, Object> params);

    default Uni<Void> updateByKeyBatch(List<V> value) {
        throw GenericFailure.notImplemented();
    }

    default Uni<Void> insertBatch(List<V> value) {
        throw GenericFailure.notImplemented();
    }

    default Uni<Void> insertBatch(List<V> value, SqlConnection sqlConnection) {
        throw GenericFailure.notImplemented();
    }

    default void insertAndForget(V value) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> updateByKey(V value) {
        throw GenericFailure.notImplemented();
    }

    default void updateAndForget(V value) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> updateByKey(V value, String statement) {
        throw GenericFailure.notImplemented();
    }

    default Uni<Void> deleteByKey(K key) {
        throw GenericFailure.notImplemented();
    }

    default Uni<Void> deleteByKey(K key, SqlConnection sqlConnection) {
        throw GenericFailure.notImplemented();
    }

    Uni<Void> deleteByKeyBatch(List<K> keys, SqlConnection sqlConnection);

    default Uni<Integer> count(Q query) {
        throw GenericFailure.notImplemented();
    }


    Uni<Integer> count(String query, Map<String, Object> params);

    default Uni<Void> deleteQuery(Q query, SqlConnection sqlConnection) {
        throw GenericFailure.notImplemented();
    }

    Uni<Void> deleteQuery(Q query);

    default void deleteAndForget(K key) {
        throw GenericFailure.notImplemented();
    }

    default void deleteAndForget(String statement, Map<String, Object> params) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> deleteUnique(String statement) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> deleteUnique(String statement, Map<String, Object> map) {
        throw GenericFailure.notImplemented();
    }

    default Uni<V> deleteUnique(String statement, Map<String, Object> map, SqlConnection sqlConnection) {
        throw GenericFailure.notImplemented();
    }

    default Uni<List<V>> query(String statement, Map<String, Object> params, SqlConnection sqlConnection) {
        throw GenericFailure.notImplemented();
    }


    Uni<Void> stream(Consumer<V> handler, Q query);


    default <T> Uni<T> transaction(Function<SqlConnection, Uni<T>> function) {
        throw GenericFailure.notImplemented();
    }
}
