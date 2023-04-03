

package io.vertx.eventx.sql;

import io.smallrye.mutiny.Uni;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.mutiny.sqlclient.templates.SqlTemplate;
import io.vertx.eventx.sql.commands.GenerateQueryCommand;
import io.vertx.eventx.sql.models.Query;
import io.vertx.eventx.sql.models.RecordRepository;
import io.vertx.eventx.sql.models.RepositoryRecord;
import io.vertx.eventx.sql.models.RepositoryRecordKey;
import io.vertx.pgclient.spi.PgDriver;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.impl.SqlClientInternal;
import io.vertx.sqlclient.spi.ConnectionFactory;
import io.vertx.sqlclient.spi.Driver;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public final class Repository<K extends RepositoryRecordKey, V extends RepositoryRecord<V>, Q extends Query> implements RecordRepository<K, V, Q> {

  private final Logger logger;
  private final RepositoryHandler repositoryHandler;
  private final QueryGeneratorMapper<K, V, Q> queryGeneratorMapper;

  public Repository(
    final RecordMapper<K, V, Q> mapper,
    final RepositoryHandler repositoryHandler
  ) {
    this.queryGeneratorMapper = new QueryGeneratorMapper<>(mapper);
    this.repositoryHandler = repositoryHandler;
    this.logger = LoggerFactory.getLogger(queryGeneratorMapper.actualRecordType);
  }

  @Override
  public Uni<V> selectUnique(String statement) {
    logOperation("Selecting", statement);
    return repositoryHandler.handleSelectUnique(queryGeneratorMapper.actualRecordType, logger).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(Map.of())
    );
  }

  @Override
  public Uni<V> selectUnique(String statement, Map<String, Object> paramMap) {
    logOperation("Selecting", statement, paramMap);
    return repositoryHandler.handleSelectUnique(queryGeneratorMapper.actualRecordType, logger).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(paramMap)
    );
  }

  @Override
  public Uni<V> selectUnique(String statement, Map<String, Object> paramMap, SqlConnection sqlConnection) {
    logOperation("Selecting", statement, paramMap);
    return repositoryHandler.handleSelectUnique(queryGeneratorMapper.actualRecordType, logger).apply(
      () -> SqlTemplate.forQuery(sqlConnection, statement)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(paramMap)
    );
  }

  @Override
  public Uni<V> selectByKey(K key) {
    logOperation("Selecting", key);
    return repositoryHandler.handleSelectUnique(queryGeneratorMapper.actualRecordType,logger).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), queryGeneratorMapper.selectByKeyStatement)
        .mapFrom(queryGeneratorMapper.keyTupleMapper)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(key)
    );
  }

  @Override
  public Uni<V> selectByKey(K key, SqlConnection sqlConnection) {
    logOperation("Selecting", key);
    return repositoryHandler.handleSelectUnique(queryGeneratorMapper.actualRecordType, logger).apply(
      () -> SqlTemplate.forQuery(sqlConnection, queryGeneratorMapper.selectByKeyStatement)
        .mapFrom(queryGeneratorMapper.keyTupleMapper)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(key)
    );
  }

  @Override
  public Uni<List<V>> query(Q query) {
    logOperation("Querying", query);
    final var tuple = queryGeneratorMapper.generateQuery(new GenerateQueryCommand<Q>(QueryStatementType.SELECT, query));
    return repositoryHandler.handleQuery(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), tuple.getItem1())
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(tuple.getItem2())
    );
  }

  @Override
  public Uni<List<V>> query(Q query, SqlConnection sqlConnection) {
    logOperation("Querying", query);
    final var tuple = queryGeneratorMapper.generateQuery(new GenerateQueryCommand<Q>(QueryStatementType.SELECT, query));
    return repositoryHandler.handleQuery(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(sqlConnection, tuple.getItem1())
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(tuple.getItem2())
    );
  }

  @Override
  public Uni<List<V>> query(String query, SqlConnection sqlConnection) {
    logOperation("Querying", query);
    return repositoryHandler.handleQuery(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(sqlConnection, query)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(Map.of())
    );
  }

  @Override
  public Uni<Integer> count(Q query) {
    logger.debug("Performing query ->" + query);
    final var tuple = queryGeneratorMapper.generateQuery(new GenerateQueryCommand<Q>(QueryStatementType.COUNT, query));
    return repositoryHandler.handleSelectUnique(Integer.class, logger).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), tuple.getItem1())
        .mapTo(queryGeneratorMapper.rowCounterMapper)
        .execute(tuple.getItem2())
    );
  }

  @Override
  public Uni<Integer> count(String query, Map<String, Object> params) {
    logOperation("Counting", query, params);
    return repositoryHandler.handleSelectUnique(Integer.class, logger).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), query)
        .mapTo(queryGeneratorMapper.rowCounterMapper)
        .execute(params)
    );
  }

  @Override
  public Uni<Void> exists(K key) {
    logOperation("Exists", key);
    return repositoryHandler.handleExists(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), queryGeneratorMapper.selectByKeyStatement)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .mapFrom(queryGeneratorMapper.keyTupleMapper)
        .execute(key)
    );
  }

  @Override
  public Uni<Void> exists(K key, String statement) {
    logOperation("Exists", key);
    return repositoryHandler.handleExists(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .mapFrom(queryGeneratorMapper.keyTupleMapper)
        .execute(key)
    );
  }

  @Override
  public Uni<V> insert(V value) {
    logOperation("Inserting", value);
    return repositoryHandler.handleInsert(value).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), queryGeneratorMapper.insertStatement)
          .mapFrom(queryGeneratorMapper.recordTupleMapper)
          .execute(value)
      )
      // todo replace with values in db
      .replaceWith(value);
  }

  @Override
  public Uni<V> insert(V value, SqlConnection connection) {
    logOperation("Inserting", value);
    return repositoryHandler.handleInsert(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(connection, queryGeneratorMapper.insertStatement)
          .mapFrom(queryGeneratorMapper.recordTupleMapper)
          .execute(value)
      )
      .replaceWith(value);
  }

  @Override
  public Uni<V> updateByKey(V value, SqlConnection sqlConnection) {
    logOperation("Updating", value);
    return repositoryHandler.handleUpdateByKey(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(sqlConnection, queryGeneratorMapper.updateByKeyStatement)
        .mapFrom(queryGeneratorMapper.recordTupleMapper)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(value)
    );
  }

  @Override
  public Uni<Void> updateByKeyBatch(List<V> value, SqlConnection sqlConnection) {
    logOperations("Updating", value);
    return repositoryHandler.handleUpdate(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), queryGeneratorMapper.updateByKeyStatement)
          .mapFrom(queryGeneratorMapper.recordTupleMapper)
          .executeBatch(value)
      )
      .replaceWithVoid();
  }


  @Override
  public Uni<Void> updateBatch(String query, List<Map<String, Object>> params, SqlConnection sqlConnection) {
    logOperations("Updating", query, params);
    return repositoryHandler.handleUpdate(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(sqlConnection, query)
          .executeBatch(params)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> updateBatch(String query, List<Map<String, Object>> params) {
    logOperations("Updating", query, params);
    return repositoryHandler.handleUpdate(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), query)
          .executeBatch(params)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> update(String query, Map<String, Object> params, SqlConnection sqlConnection) {
    logOperation("Updating", query, params);
    return repositoryHandler.handleUpdate(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(sqlConnection, query)
          .execute(params)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> update(String query, Map<String, Object> params) {
    logOperation("Updating", query, params);
    return repositoryHandler.handleUpdate(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), query)
          .execute(params)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> updateByKeyBatch(List<V> value) {
    logOperations("Updating", value);
    return repositoryHandler.handleUpdate(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), queryGeneratorMapper.updateByKeyStatement)
          .mapFrom(queryGeneratorMapper.recordTupleMapper)
          .executeBatch(value)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> insertBatch(List<V> value) {
    logOperations("Inserting", value);
    return repositoryHandler.handleInsert(value).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), queryGeneratorMapper.insertStatement)
          .mapFrom(queryGeneratorMapper.recordTupleMapper)
          .executeBatch(value)
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> insertBatch(List<V> value, SqlConnection sqlConnection) {
    logOperations("Inserting", value);
    return repositoryHandler.handleInsert(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(sqlConnection, queryGeneratorMapper.insertStatement)
          .mapFrom(queryGeneratorMapper.recordTupleMapper)
          .executeBatch(value)
      )
      .replaceWithVoid();
  }


  @Override
  public void insertAndForget(V value) {
    logOperation("Inserting", value);
    repositoryHandler.handleInsert(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), queryGeneratorMapper.insertStatement)
          .mapFrom(queryGeneratorMapper.recordTupleMapper)
          .execute(value)
      )
      .subscribe()
      .with(
        n -> logger.info("inserted -> " + value)
        , throwable -> logger.error("failed to insert -> " + value, throwable)
      );
  }

  @Override
  public Uni<V> updateByKey(V value) {
    logOperation("Updating", value);
    return repositoryHandler.handleUpdate(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), queryGeneratorMapper.updateByKeyStatement)
          .mapFrom(queryGeneratorMapper.recordTupleMapper)
          .execute(value)
      )
      .map(version -> value.with(value.baseRecord().withVersion(version)));
  }

  @Override
  public void updateAndForget(V value) {
    logOperation("Updating", value);
    repositoryHandler.handleUpdate(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), queryGeneratorMapper.updateByKeyStatement)
          .mapFrom(queryGeneratorMapper.recordTupleMapper)
          .execute(value)
      )
      .subscribe()
      .with(
        n -> logger.info("Deleted record -> " + value)
        , throwable -> logger.error("failed to delete key -> " + value, throwable)
      );
  }

  @Override
  public Uni<V> updateByKey(V value, String statement) {
    logOperation("Updating", value);
    return repositoryHandler.handleUpdate(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
          .mapFrom(queryGeneratorMapper.recordTupleMapper)
          .execute(value)
      )
      .map(version -> value.with(value.baseRecord().withVersion(version)));
  }

  @Override
  public Uni<Void> deleteQuery(Q query, SqlConnection sqlConnection) {
    logOperation("Deleting", query);
    final var tuple = queryGeneratorMapper.generateQuery(new GenerateQueryCommand<Q>(QueryStatementType.DELETE, query));
    return repositoryHandler.handleDelete(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(sqlConnection, tuple.getItem1())
        .execute(tuple.getItem2())
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteQuery(Q query) {
    logOperation("Deleting", query);
    final var tuple = queryGeneratorMapper.generateQuery(new GenerateQueryCommand<Q>(QueryStatementType.DELETE, query));
    return repositoryHandler.handleDelete(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), tuple.getItem1())
        .execute(tuple.getItem2())
    ).replaceWithVoid();
  }


  @Override
  public Uni<Void> deleteByKey(K key) {
    logOperation("Deleting", key);
    return repositoryHandler.handleDelete(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), queryGeneratorMapper.deleteByKeyStatement)
        .mapFrom(queryGeneratorMapper.keyTupleMapper)
        .execute(key)
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteByKey(K key, SqlConnection sqlConnection) {
    logOperation("Deleting", key);
    return repositoryHandler.handleDelete(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(sqlConnection, queryGeneratorMapper.deleteByKeyStatement)
        .mapFrom(queryGeneratorMapper.keyTupleMapper)
        .execute(key)
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteByKeyBatch(List<K> keys, SqlConnection sqlConnection) {
    logOperations("Deleting", keys);
    return repositoryHandler.handleDelete(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(sqlConnection, queryGeneratorMapper.deleteByKeyStatement)
        .mapFrom(queryGeneratorMapper.keyTupleMapper)
        .executeBatch(keys)
    ).replaceWithVoid();
  }

  @Override
  public void deleteAndForget(K key) {
    logOperation("Deleting", key);
    repositoryHandler.handleDelete(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), queryGeneratorMapper.deleteByKeyStatement)
          .mapFrom(queryGeneratorMapper.keyTupleMapper)
          .execute(key)
      )
      .subscribe()
      .with(
        id -> logger.info("Deleted record with key -> " + key)
        , throwable -> logger.error("Failed to delete record with key ->  " + key, throwable)
      );
  }

  @Override
  public void deleteAndForget(String statement, Map<String, Object> paramMap) {
    logOperation("Deleting", statement, paramMap);
    repositoryHandler.handleDelete(queryGeneratorMapper.actualRecordType).apply(
        () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
          .execute(paramMap)
      )
      .subscribe()
      .with(
        id -> logger.debug("Deleted records that match type -> " + statement + " with params -> " + paramMap)
        , throwable -> logger.error("Failed to delete records that match type -> " + statement + " with params -> " + paramMap, throwable)
      );
  }

  @Override
  public Uni<V> deleteUnique(String statement) {
    logOperation("Deleting", statement);
    return selectUnique(statement)
      .flatMap(item -> repositoryHandler.handleDelete(queryGeneratorMapper.actualRecordType).apply(
          () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
            .execute(Map.of())
        ).replaceWith(item)
      );
  }

  @Override
  public Uni<V> deleteUnique(String statement, Map<String, Object> paramMap) {
    logOperation("Deleting", statement, paramMap);
    return selectUnique(statement, paramMap)
      .flatMap(item -> repositoryHandler.handleDelete(queryGeneratorMapper.actualRecordType).apply(
          () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
            .execute(paramMap)
        ).replaceWith(item)
      );
  }

  @Override
  public Uni<V> deleteUnique(String statement, Map<String, Object> paramMap, SqlConnection sqlConnection) {
    logOperation("Deleting", statement, paramMap);
    return selectUnique(statement, paramMap)
      .flatMap(item -> repositoryHandler.handleDelete(queryGeneratorMapper.actualRecordType).apply(
          () -> SqlTemplate.forQuery(sqlConnection, statement)
            .execute(paramMap)
        ).replaceWith(item)
      );
  }

  @Override
  public Uni<List<V>> query(final String query) {
    return repositoryHandler.handleQuery(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), query)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(Map.of())
    );
  }

  @Override
  public Uni<List<V>> query(final String statement, final Map<String, Object> paramMap) {
    logOperation("Querying", statement, paramMap);
    return repositoryHandler.handleQuery(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(repositoryHandler.sqlClient(), statement)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(paramMap)
    );
  }


  @Override
  public Uni<List<V>> query(final String statement, final Map<String, Object> paramMap, SqlConnection sqlConnection) {
    logOperation("Querying", statement, paramMap);
    return repositoryHandler.handleQuery(queryGeneratorMapper.actualRecordType).apply(
      () -> SqlTemplate.forQuery(sqlConnection, statement)
        .mapTo(queryGeneratorMapper.recordRowMapper)
        .execute(paramMap)
    );
  }

  private Uni<Void> stream(Uni<Void> lock, Consumer<V> handler, String statement, Tuple arguments) {
    logOperation("Streaming", statement, arguments);
    return repositoryHandler.handleStreamProcessing(repositoryHandler.pgPool(), lock, statement, queryGeneratorMapper.recordRowMapper, handler, arguments)
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> stream(Consumer<V> handler, Q query) {
    final var tuple = queryGeneratorMapper.generateQuery(new GenerateQueryCommand<>(QueryStatementType.SELECT, query));
    final var template = io.vertx.sqlclient.templates.impl.SqlTemplate.create(
      new SqlClientInternal() {
        @Override
        public Driver driver() {
         return PgDriver.INSTANCE;
        }

        @Override
        public void group(Handler<SqlClient> handler) {

        }

        @Override
        public io.vertx.sqlclient.Query<RowSet<Row>> query(String s) {
          return null;
        }

        @Override
        public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
          return null;
        }

        @Override
        public PreparedQuery<RowSet<Row>> preparedQuery(String s, PrepareOptions prepareOptions) {
          return null;
        }

        @Override
        public void close(Handler<AsyncResult<Void>> handler) {

        }

        @Override
        public Future<Void> close() {
          return null;
        }
      },
      tuple.getItem1()
    );
    final var params = Tuple.newInstance(template.mapTuple(tuple.getItem2()));
    final var statement = template.getSql();
    return stream(Uni.createFrom().voidItem(), handler, statement, params);
  }


  @Override
  public <T> Uni<T> transaction(Function<SqlConnection, Uni<T>> function) {
    return repositoryHandler().pgPool().withTransaction(function);
  }

  public RepositoryHandler repositoryHandler() {
    return repositoryHandler;
  }


  private void logOperation(String operation, String statement, Map<String, Object> paramMap) {
    if (logger.isDebugEnabled()) {
      logger.debug(operation + " record with statement -> " + new JsonObject().put("statement", statement).put("params", paramMap).encodePrettily());
    }
  }

  private void logOperation(String operation, String statement, Tuple paramMap) {
    if (logger.isDebugEnabled())
      logger.debug(operation + " record with statement -> " + new JsonObject().put("statement", statement).put("params", paramMap).encodePrettily());
  }

  private void logOperations(String operation, String statement, List<Map<String, Object>> paramMap) {
    if (logger.isDebugEnabled())
      logger.debug(operation + " record with statement -> " + new JsonObject().put("statement", statement).put("params", paramMap).encodePrettily());
  }

  private void logOperation(String operation, String statement) {
    if (logger.isDebugEnabled())
      logger.debug(operation + " record with statement -> " + new JsonObject().put("statement", statement).encodePrettily());
  }

  private void logOperation(String operation, Object object) {
    if (logger.isDebugEnabled())
      logger.debug(operation + " record -> " + JsonObject.mapFrom(object).encodePrettily());
  }

  private void logOperations(String operation, List<?> object) {
    if (logger.isDebugEnabled())
      logger.debug(operation + " records -> " + new JsonArray(object).encodePrettily());
  }

}
