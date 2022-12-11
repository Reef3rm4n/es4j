

package io.vertx.skeleton.orm;

import io.vertx.skeleton.models.OrmGenericException;
import io.vertx.skeleton.models.Query;
import io.vertx.skeleton.models.RepositoryRecord;
import io.vertx.skeleton.models.Tenant;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.shareddata.Lock;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Tuple;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public interface RecordRepository<K, V extends RepositoryRecord<V>, Q extends Query> {

  RepositoryMapper<K, V, Q> mapper();

  default Uni<V> selectUnique(String statement) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<V> selectUnique(String statement, Map<String, Object> map) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<V> selectUnique(String statement, Map<String, Object> map, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  default Multi<V> selectByTenant(Tenant tenant) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<V> selectByKey(K key) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<V> selectByKey(K key, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<V> selectById(Long id) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<V> selectById(Long id, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<List<V>> query(Q query) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<List<V>> query(Q query, String statement) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<List<V>> query(String query) {
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

  default Uni<V> insertWithFallBack(V value, Uni<V> fallback) {
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



  Uni<Void> insertOrUpdate(V value, K key);

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

  default Uni<V> updateById(V value) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<V> updateById(V value, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> updateByIdBatch(List<V> value, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> updateByIdBatch(List<V> value) {
    throw OrmGenericException.notImplemented();
  }

  default void updateAndForget(V value) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<V> updateByKey(V value, String statement) {
    throw OrmGenericException.notImplemented();
  }


  /**
   * Deletes record
   *
   * @param key
   * @return old value
   */
  default Uni<V> deleteByKeyReturningOldValue(K key) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> deleteByKey(K key) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> deleteByKey(K key, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  Uni<Void> deleteByKeyBatch(List<K> keys, SqlConnection sqlConnection);

  /**
   * Deletes record
   *
   * @param key
   * @return old value
   */
  default Uni<V> deleteByKeyReturningOldValue(K key, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Integer> count(Q query) {
    throw OrmGenericException.notImplemented();
  }


  Uni<Integer> count(String query, Map<String, Object> params);

  /**
   * Deletes record
   *
   * @param key
   * @return old value
   */
  default Uni<List<V>> deleteQueryReturningDeletedValues(Q key, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }


  default Uni<Void> deleteQuery(Q query, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  Uni<Void> deleteQuery(Q query);

  default Uni<List<V>> deleteQueryReturningDeletedValues(Q key) {
    throw OrmGenericException.notImplemented();
  }


  default void deleteAndForget(K key) {
    throw OrmGenericException.notImplemented();
  }

  default void deleteAndForget(String statement, Map<String, Object> params) {
    throw OrmGenericException.notImplemented();
  }

  /**
   * Deletes record
   *
   * @return old value
   */
  default Uni<V> deleteUnique(String statement) {
    throw OrmGenericException.notImplemented();
  }

  /**
   * Deletes record
   *
   * @return old value
   */
  default Uni<V> deleteUnique(String statement, Map<String, Object> map) {
    throw OrmGenericException.notImplemented();
  }

  /**
   * Deletes record
   *
   * @return old value
   */
  default Uni<V> deleteUnique(String statement, Map<String, Object> map, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  /**
   * Deletes record
   *
   * @return old value
   */

  default Uni<Void> deleteById(Long id) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> deleteById(Long id, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> deleteByIdBatch(List<Long> ids, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> deleteByIdBatch(List<Long> ids) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<List<V>> query(String statement, Map<String, Object> params, SqlConnection sqlConnection) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> stream(Lock lock, Consumer<V> handler) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> stream(Uni<Void> lock, Consumer<V> handler, String statement, Tuple arguments) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> stream(Lock lock, Consumer<V> handler, String statement) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> stream(Lock lock, Consumer<V> handler, String statement, Tuple arguments) {
    throw OrmGenericException.notImplemented();
  }

  default Uni<Void> stream(Lock lock, Consumer<V> handler, String statement, Tuple arguments, Integer batchSize) {
    throw OrmGenericException.notImplemented();
  }

  default public <T> Uni<T> transaction(Function<SqlConnection, Uni<T>> function) {
    throw OrmGenericException.notImplemented();
  }
}
