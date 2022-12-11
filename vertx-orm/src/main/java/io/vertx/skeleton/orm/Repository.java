

package io.vertx.skeleton.orm;


import io.vertx.skeleton.models.RepositoryRecord;
import io.vertx.skeleton.models.Query;
import io.vertx.skeleton.models.Tenant;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.mutiny.core.shareddata.Lock;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.mutiny.sqlclient.templates.SqlTemplate;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;


public final class Repository<K, V extends RepositoryRecord<V>, Q extends Query> implements RecordRepository<K, V, Q> {

  private static final Logger logger = LoggerFactory.getLogger(Repository.class);
  private final String UPDATE_BY_ID;
  private final String SELECT_BY_TENANT;
  private final String SELECT_BY_ID;
  private final RepositoryMapper<K, V, Q> mapper;
  private final RepositoryHandler repositoryHandler;
  private final String SELECT_BY_KEY;
  private final String UPDATE_BY_KEY;
  private final String INSERT;
  private final String DELETE_BY_KEY;
  private final String DELETE_BY_ID;

  public Repository(
    RepositoryMapper<K, V, Q> mapper,
    RepositoryHandler repositoryHandler
  ) {
    this.mapper = mapper;
    this.repositoryHandler = repositoryHandler;
    this.SELECT_BY_KEY = mapper.selectByKey();
    this.SELECT_BY_ID = mapper.selectById();
    this.SELECT_BY_TENANT = mapper.selectByTenant();
    this.UPDATE_BY_KEY = mapper.updateByKey();
    this.UPDATE_BY_ID = mapper.updateById();
    this.INSERT = mapper.insert();
    this.DELETE_BY_KEY = mapper.deleteByKey();
    this.DELETE_BY_ID = mapper.deleteById();
  }


  @Override
  public Uni<V> selectUnique(String statement) {
    return repositoryHandler.handleSelectUnique(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
        .mapTo(mapper.rowMapper())
        .execute(Map.of())
    );
  }

  @Override
  public Uni<V> selectUnique(String statement, Map<String, Object> map) {
    return repositoryHandler.handleSelectUnique(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
        .mapTo(mapper.rowMapper())
        .execute(map)
    );
  }

  @Override
  public Uni<V> selectUnique(String statement, Map<String, Object> map, SqlConnection sqlConnection) {
    return repositoryHandler.handleSelectUnique(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(sqlConnection, statement)
        .mapTo(mapper.rowMapper())
        .execute(map)
    );
  }

  @Override
  public Multi<V> selectByTenant(Tenant tenant) {
    return repositoryHandler.handleQueryMultiStream(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), SELECT_BY_TENANT)
        .mapTo(mapper.rowMapper())
        .execute(Map.of("tenant", tenant.generateString()))
    );
  }

  @Override
  public Uni<V> selectByKey(K key) {
    logger.debug("Selecting by key " + key);
    return repositoryHandler.handleSelectUnique(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), SELECT_BY_KEY)
        .mapFrom(mapper.keyMapper())
        .mapTo(mapper.rowMapper())
        .execute(key)
    );
  }

  @Override
  public Uni<V> selectByKey(K key, SqlConnection sqlConnection) {
    return repositoryHandler.handleSelectUnique(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(sqlConnection, SELECT_BY_KEY)
        .mapFrom(mapper.keyMapper())
        .mapTo(mapper.rowMapper())
        .execute(key)
    );
  }

  @Override
  public Uni<V> selectById(Long id) {
    return repositoryHandler.handleSelectUnique(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), SELECT_BY_ID)
        .mapTo(mapper.rowMapper())
        .execute(Map.of("id", id))
    );
  }

  @Override
  public Uni<V> selectById(Long id, SqlConnection sqlConnection) {
    return repositoryHandler.handleSelectUnique(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(sqlConnection, SELECT_BY_ID)
        .mapTo(mapper.rowMapper())
        .execute(Map.of("id", id))
    );
  }

  @Override
  public Uni<List<V>> query(Q query) {
    logger.debug("Performing query with object :" + query);
    final var tuple = mapper.queryStatementAndParams(query, false);
    return repositoryHandler.handleQuery(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), tuple.getItem1())
        .mapTo(mapper.rowMapper())
        .execute(tuple.getItem2())
    );
  }

  @Override
  public Uni<Integer> count(Q query) {
    logger.debug("Performing query with object :" + query);
    final var tuple = mapper.count(query);
    return repositoryHandler.handleSelectUnique(Integer.class).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), tuple.getItem1())
        .mapTo(mapper.countMapper())
        .execute(tuple.getItem2())
    );
  }

  @Override
  public Uni<Integer> count(String query, Map<String, Object> params) {
    logger.debug("Performing query with object :" + query);
    return repositoryHandler.handleSelectUnique(Integer.class).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), query)
        .mapTo(mapper.countMapper())
        .execute(params)
    );
  }


  @Override
  public Uni<List<V>> query(Q query, String statement) {
    logger.debug("Query statement :" + statement);
    final var tuple = mapper.queryStatementAndParams(query,false);
    return repositoryHandler.handleQuery(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
        .mapTo(mapper.rowMapper())
        .execute(tuple.getItem2())
    );
  }


  @Override
  public Uni<Void> exists(K key) {
    return repositoryHandler.handleExists(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), SELECT_BY_KEY)
        .mapTo(mapper.rowMapper())
        .mapFrom(mapper.keyMapper())
        .execute(key)
    );
  }

  @Override
  public Uni<Void> exists(K key, String statement) {
    return repositoryHandler.handleExists(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
        .mapTo(mapper.rowMapper())
        .mapFrom(mapper.keyMapper())
        .execute(key)
    );
  }

  @Override
  public Uni<V> insert(V value) {
    logger.debug("Inserting record " + value);
    return repositoryHandler.handleInsert(value).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), INSERT)
          .mapFrom(mapper.tupleMapper())
          .execute(value)
      )
      .map(id -> value.with(value.persistedRecord().withId(id)));
  }

  @Override
  public Uni<V> insertWithFallBack(V value, Uni<V> fallback) {
    logger.debug("Inserting record " + value);
    return repositoryHandler.handleInsert(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), INSERT)
          .mapFrom(mapper.tupleMapper())
          .execute(value)
      )
      .map(id -> value.with(value.persistedRecord().withId(id)))
      .onFailure().call(throwable -> {
          logger.warn("Insert failed, using fallback", throwable);
          return fallback;
        }
      );
  }

  @Override
  public Uni<V> insert(V value, SqlConnection connection) {
    logger.debug("Inserting record " + value);
    return repositoryHandler.handleInsert(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(connection, INSERT)
          .mapFrom(mapper.tupleMapper())
          .execute(value)
      )
      .map(id -> value.with(value.persistedRecord().withId(id)));
  }

  @Override
  public Uni<V> updateByKey(V value, SqlConnection sqlConnection) {
    logger.debug("Updating record " + value);
    return repositoryHandler.handleUpdateByKey(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(sqlConnection, UPDATE_BY_KEY)
        .mapFrom(mapper.tupleMapper())
        .mapTo(mapper.rowMapper())
        .execute(value)
    );
  }

  @Override
  public Uni<Void> updateByKeyBatch(List<V> value, SqlConnection sqlConnection) {
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), UPDATE_BY_KEY)
          .mapFrom(mapper.tupleMapper())
          .executeBatch(value)
      )
      .replaceWithVoid();
  }


  @Override
  public Uni<Void> insertOrUpdate(V value, K key) {
    logger.debug("Updating record " + value);
    return selectByKey(key).onItemOrFailure().transformToUni(
      (item, failure) -> {
        if (failure != null) {
          return insert(value);
        } else {
          return updateByKey(value.with(item.persistedRecord()));
        }
      }
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> updateBatch(String query, List<Map<String, Object>> params, SqlConnection sqlConnection) {
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(sqlConnection, query)
          .executeBatch(params)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> updateBatch(String query, List<Map<String, Object>> params) {
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), query)
          .executeBatch(params)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> update(String query, Map<String, Object> params, SqlConnection sqlConnection) {
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(sqlConnection, query)
          .execute(params)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> update(String query, Map<String, Object> params) {
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), query)
          .execute(params)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> updateByKeyBatch(List<V> value) {
    logger.debug("Updating records " + value);
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), UPDATE_BY_KEY)
          .mapFrom(mapper.tupleMapper())
          .executeBatch(value)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> insertBatch(List<V> value) {
    return repositoryHandler.handleInsert(value).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), INSERT)
          .mapFrom(mapper.tupleMapper())
          .executeBatch(value)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> insertBatch(List<V> value, SqlConnection sqlConnection) {
    return repositoryHandler.handleInsert(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(sqlConnection, INSERT)
          .mapFrom(mapper.tupleMapper())
          .executeBatch(value)
      )
      .replaceWithVoid();
  }


  @Override
  public void insertAndForget(V value) {
    logger.debug("Inserting record " + value);
    repositoryHandler.handleInsert(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), INSERT)
          .mapFrom(mapper.tupleMapper())
          .execute(value)
      )
      .subscribe()
      .with(
        n -> logger.info("inserted :" + value.persistedRecord())
        , throwable -> logger.error("failed to insert " + value, throwable)
      );
  }

  @Override
  public Uni<V> updateByKey(V value) {
    logger.debug("Updating record " + value);
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), UPDATE_BY_KEY)
          .mapFrom(mapper.tupleMapper())
          .execute(value)
      )
      .map(version -> value.with(value.persistedRecord().withVersion(version)));
  }

  @Override
  public Uni<V> updateById(V value) {
    logger.debug("Updating record " + value);
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), UPDATE_BY_ID)
          .mapFrom(mapper.tupleMapper())
          .execute(value)
      )
      .map(id -> value.with(value.persistedRecord().withVersion(id)));
  }

  @Override
  public Uni<V> updateById(V value, SqlConnection sqlConnection) {
    logger.debug("Updating record " + value);
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(sqlConnection, UPDATE_BY_ID)
          .mapFrom(mapper.tupleMapper())
          .execute(value)
      )
      .map(id -> value.with(value.persistedRecord().withVersion(id)));
  }

  @Override
  public Uni<Void> updateByIdBatch(List<V> value, SqlConnection sqlConnection) {
    logger.debug("Updating record " + value);
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(sqlConnection, UPDATE_BY_ID)
          .mapFrom(mapper.tupleMapper())
          .executeBatch(value)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> updateByIdBatch(List<V> value) {
    logger.debug("Updating record " + value);
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), UPDATE_BY_ID)
          .mapFrom(mapper.tupleMapper())
          .executeBatch(value)
      )
      .replaceWithVoid();
  }

  @Override
  public void updateAndForget(V value) {
    logger.debug("Updating record " + value);
    repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), UPDATE_BY_KEY)
          .mapFrom(mapper.tupleMapper())
          .execute(value)
      )
      .subscribe()
      .with(
        n -> logger.info("deleted key :" + value)
        , throwable -> logger.error("failed to delete key " + value, throwable)
      );
  }

  @Override
  public Uni<V> updateByKey(V value, String statement) {
    logger.debug("Updating record " + value);
    return repositoryHandler.handleUpdate(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
          .mapFrom(mapper.tupleMapper())
          .execute(value)
      )
      .map(version -> value.with(value.persistedRecord().withVersion(version)));
  }

  @Override
  public Uni<List<V>> deleteQueryReturningDeletedValues(Q query, SqlConnection sqlConnection) {
    logger.debug("Deleting records " + query);
    final var tuple = mapper.queryStatementAndParams(query, true);
    return query(query).flatMap(items -> repositoryHandler.handleDelete(mapper.valueClass()).apply(
          () -> SqlTemplate.forQuery(sqlConnection, tuple.getItem1())
            .execute(tuple.getItem2())
        )
        .replaceWith(items)
    );
  }

  @Override
  public Uni<Void> deleteQuery(Q query, SqlConnection sqlConnection) {
    logger.debug("Deleting records " + query);
    final var tuple = mapper.queryStatementAndParams(query, true);
    return repositoryHandler.handleDelete(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(sqlConnection, tuple.getItem1())
        .execute(tuple.getItem2())
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteQuery(Q query) {
    logger.debug("Deleting records " + query);
    final var tuple = mapper.queryStatementAndParams(query, true);
    return repositoryHandler.handleDelete(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), tuple.getItem1())
        .execute(tuple.getItem2())
    ).replaceWithVoid();
  }

  @Override
  public Uni<List<V>> deleteQueryReturningDeletedValues(Q query) {
    logger.debug("Deleting records " + query);
    final var tuple = mapper.queryStatementAndParams(query, true);
    return query(query)
      .flatMap(items -> repositoryHandler.handleDelete(mapper.valueClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), tuple.getItem1())
          .execute(tuple.getItem2())
      ).replaceWith(items)
      );
  }

  @Override
  public Uni<V> deleteByKeyReturningOldValue(K key) {
    logger.debug("Deleting record " + key);
    return selectByKey(key)
      .flatMap(item -> repositoryHandler.handleDelete(mapper.keyClass()).apply(
          () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), DELETE_BY_KEY)
            .mapFrom(mapper.keyMapper())
            .execute(key)
        ).replaceWith(item)
      );
  }

  @Override
  public Uni<Void> deleteByKey(K key) {
    logger.debug("Deleting record " + key);
    return repositoryHandler.handleDelete(mapper.keyClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), DELETE_BY_KEY)
        .mapFrom(mapper.keyMapper())
        .execute(key)
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteByKey(K key, SqlConnection sqlConnection) {
    logger.debug("Deleting record " + key);
    return repositoryHandler.handleDelete(mapper.keyClass()).apply(
      () -> SqlTemplate.forQuery(sqlConnection, DELETE_BY_KEY)
        .mapFrom(mapper.keyMapper())
        .execute(key)
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteByKeyBatch(List<K> keys, SqlConnection sqlConnection) {
    logger.debug("Deleting record " + keys);
    return repositoryHandler.handleDelete(mapper.keyClass()).apply(
      () -> SqlTemplate.forQuery(sqlConnection, DELETE_BY_KEY)
        .mapFrom(mapper.keyMapper())
        .executeBatch(keys)
    ).replaceWithVoid();
  }

  @Override
  public Uni<V> deleteByKeyReturningOldValue(K key, SqlConnection sqlConnection) {
    logger.debug("Deleting record " + key);
    return selectByKey(key, sqlConnection)
      .flatMap(item -> repositoryHandler.handleDelete(mapper.keyClass()).apply(
          () -> SqlTemplate.forQuery(sqlConnection, DELETE_BY_KEY)
            .mapFrom(mapper.keyMapper())
            .execute(key)
        ).replaceWith(item)
      );
  }


  @Override
  public void deleteAndForget(K key) {
    logger.debug("Deleting record " + key);
    repositoryHandler.handleDelete(mapper.keyClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), DELETE_BY_KEY)
          .mapFrom(mapper.keyMapper())
          .execute(key)
      )
      .subscribe()
      .with(
        id -> logger.info("deleted key :" + key)
        , throwable -> logger.error("failed to delete key " + key, throwable)
      );
  }

  @Override
  public void deleteAndForget(String statement, Map<String, Object> params) {
    repositoryHandler.handleDelete(mapper.keyClass()).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
          .execute(params)
      )
      .subscribe()
      .with(
        id -> logger.debug("deleted")
        , throwable -> logger.error("failed to delete", throwable)
      );
  }

  @Override
  public Uni<V> deleteUnique(String statement) {
    logger.debug("Deleting record " + statement);
    return selectUnique(statement)
      .flatMap(item -> repositoryHandler.handleDelete(mapper.keyClass()).apply(
          () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
            .execute(Map.of())
        ).replaceWith(item)
      );
  }

  @Override
  public Uni<V> deleteUnique(String statement, Map<String, Object> map) {
    logger.debug("Deleting record " + statement);
    return selectUnique(statement, map)
      .flatMap(item -> repositoryHandler.handleDelete(mapper.keyClass()).apply(
          () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
            .execute(map)
        ).replaceWith(item)
      );
  }

  @Override
  public Uni<V> deleteUnique(String statement, Map<String, Object> map, SqlConnection sqlConnection) {
    logger.debug("Deleting record " + statement);
    return selectUnique(statement, map)
      .flatMap(item -> repositoryHandler.handleDelete(mapper.keyClass()).apply(
          () -> SqlTemplate.forQuery(sqlConnection, statement)
            .execute(map)
        ).replaceWith(item)
      );
  }

  @Override
  public Uni<Void> deleteById(Long id) {
    logger.debug("Deleting record with id " + id);
    return repositoryHandler.handleDelete(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), DELETE_BY_ID)
        .execute(Map.of("id", id))
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteById(Long id, SqlConnection sqlConnection) {
    logger.debug("Deleting record with id " + id);
    return repositoryHandler.handleDelete(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(sqlConnection, DELETE_BY_ID)
        .execute(Map.of("id", id))
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteByIdBatch(List<Long> ids, SqlConnection sqlConnection) {
    logger.debug("Deleting records with ids :" + ids);
    return repositoryHandler.handleDelete(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(sqlConnection, DELETE_BY_ID)
        .executeBatch(ids.stream().map(id -> Map.of("id", (Object) id)).toList())
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteByIdBatch(List<Long> ids) {
    logger.debug("Deleting records with ids :" + ids);
    return repositoryHandler.handleDelete(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), DELETE_BY_ID)
        .executeBatch(ids.stream().map(id -> Map.of("id", (Object) id)).toList())
    ).replaceWithVoid();
  }

  @Override
  public Uni<List<V>> query(final String query) {
    return repositoryHandler.handleQuery(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), query)
        .mapTo(mapper.rowMapper())
        .execute(Map.of())
    );
  }

  @Override
  public Uni<List<V>> query(final String statement, final Map<String, Object> params) {
    return repositoryHandler.handleQuery(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
        .mapTo(mapper.rowMapper())
        .execute(params)
    );
  }


  @Override
  public Uni<List<V>> query(final String statement, final Map<String, Object> params, SqlConnection sqlConnection) {
    return repositoryHandler.handleQuery(mapper.valueClass()).apply(
      () -> SqlTemplate.forQuery(sqlConnection, statement)
        .mapTo(mapper.rowMapper())
        .execute(params)
    );
  }

  @Override
  public Uni<Void> stream(Lock lock, Consumer<V> handler) {
    logger.debug("Streaming with statement : " + mapper.selectAll());
    return repositoryHandler.handleStreamProcessing(repositoryHandler.pgPool(), lock, mapper.selectAll(), mapper.rowMapper(), handler)
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> stream(Uni<Void> lock, Consumer<V> handler, String statement, Tuple arguments) {
    return repositoryHandler.handleStreamProcessing(repositoryHandler.pgPool(), lock, statement, mapper.rowMapper(), handler, arguments)
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> stream(Lock lock, Consumer<V> handler, String statement) {
    logger.debug("Streaming with statement : " + statement);
    return repositoryHandler.handleStreamProcessing(repositoryHandler.pgPool(), lock, statement, mapper.rowMapper(), handler)
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> stream(Lock lock, Consumer<V> handler, String statement, Tuple arguments) {
    logger.debug("Streaming with statement : " + statement);
    return repositoryHandler.handleStreamProcessing(repositoryHandler.pgPool(), lock, statement, mapper.rowMapper(), handler, arguments)
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> stream(Lock lock, Consumer<V> handler, String statement, Tuple arguments, Integer batchSize) {
    logger.debug("Streaming with statement : " + statement);
    return repositoryHandler.handleStreamProcessing(repositoryHandler.pgPool(), lock, statement, mapper.rowMapper(), handler, arguments, batchSize)
      .replaceWithVoid();
  }

  @Override
  public <T> Uni<T> transaction(Function<SqlConnection, Uni<T>> function) {
    return repositoryHandler().pgPool().withTransaction(function);
  }

  @Override
  public RepositoryMapper<K, V, Q> mapper() {
    return mapper;
  }

  public RepositoryHandler repositoryHandler() {
    return repositoryHandler;
  }

}
