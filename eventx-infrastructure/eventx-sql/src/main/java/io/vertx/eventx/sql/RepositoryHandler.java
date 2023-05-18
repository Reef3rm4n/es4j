package io.vertx.eventx.sql;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.tracing.TracingPolicy;
import io.vertx.eventx.sql.misc.SqlError;
import io.vertx.mutiny.core.Promise;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.shareddata.Lock;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.PreparedStatement;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.RowStream;
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Transaction;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgException;
import io.vertx.pgclient.SslMode;
import io.vertx.eventx.sql.exceptions.*;
import io.vertx.eventx.sql.misc.Constants;
import io.vertx.eventx.sql.misc.EnvVars;
import io.vertx.sqlclient.PoolOptions;

import java.net.ConnectException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.vertx.eventx.sql.misc.Constants.VERSION;


public record RepositoryHandler(
  Vertx vertx,
  PgPool pgPool,
  SqlClient sqlClient,
  JsonObject configuration
) {
  private static final Logger logger = LoggerFactory.getLogger(RepositoryHandler.class);

  /**
   * This uses two different types of pools :
   * 1) PgPool is a pool which operations cannot be pipelined, connections from this pool can be borrowed, used mostly for transactional purposes.
   * 2) SqlClient a pool which operations can be pipelined, connection from this pool cannot be borrowed thus transactions cannot be performed.
   *
   * @param configuration
   * @param vertx
   * @return
   */
  public static RepositoryHandler leasePool(JsonObject configuration, Vertx vertx) {
    return new RepositoryHandler(vertx, bootstrapPgPool(configuration, vertx), bootstrapSqlClient(configuration, vertx), configuration);
  }

  public static SqlClient bootstrapSqlClient(JsonObject config, Vertx vertx) {
    return PgPool.client(vertx, connectionOptions(config), pooledOptions(config));
  }

  private static PoolOptions poolOptions(JsonObject config) {
    return new PoolOptions()
      .setConnectionTimeoutUnit(TimeUnit.SECONDS)
      .setConnectionTimeout(config.getInteger("pgConnectionTimeOut", EnvVars.PG_CONNECTION_TIMEOUT))
      .setPoolCleanerPeriod(config.getInteger("pgPoolCleanerPeriod", EnvVars.PG_CLEANER_PERIOD))
      .setShared(true)
      .setName(config.getString("schema", EnvVars.SCHEMA) + "-postgres-pool");
  }

  private static PoolOptions pooledOptions(JsonObject config) {
    return new PoolOptions()
      .setConnectionTimeoutUnit(TimeUnit.SECONDS)
      .setConnectionTimeout(config.getInteger("pgConnectionTimeOut", EnvVars.PG_CONNECTION_TIMEOUT))
      .setPoolCleanerPeriod(config.getInteger("pgPoolCleanerPeriod", EnvVars.PG_CLEANER_PERIOD))
      .setShared(true)
      .setName(config.getString("schema", EnvVars.SCHEMA) + "-postgres-pooled");
  }

  public static PgPool bootstrapPgPool(JsonObject config, Vertx vertx) {
    return PgPool.pool(vertx, connectionOptions(config), poolOptions(config));
  }

  public static PgConnectOptions connectionOptions(JsonObject config) {
    return new PgConnectOptions()
      .setMetricsName(config.getString("schema", EnvVars.SCHEMA) + "-postgres")
      .setTracingPolicy(TracingPolicy.PROPAGATE)
      .setCachePreparedStatements(true)
      .setLogActivity(config.getBoolean("logActivity", EnvVars.LOG_ACTIVITY))
      // todo probe kernel being used in container and test out performance before tweaking tcp settings
//      .setTcpCork(config.getBoolean("tcpCork", EnvVars.TCP_CORK))
//      .setUseAlpn(config.getBoolean("useAlpn", EnvVars.USE_ALPN))
//      .setTcpFastOpen(config.getBoolean("tcpFastOpen", EnvVars.TCP_FAST_OPEN))
//      .setReusePort(config.getBoolean("reusePort", EnvVars.TCP_REUSE_PORT))
//      .setTcpKeepAlive(config.getBoolean("tcpKeepAlive", EnvVars.TCP_KEEP_ALIVE))
//      .setTcpNoDelay(config.getBoolean("tcpNoDelay", EnvVars.TCP_NO_DELAY))
//      .setTcpQuickAck(config.getBoolean("tcpQuickAck", EnvVars.TCP_QUICK_ACK))
      .setReconnectAttempts(config.getInteger("pgReconnect", EnvVars.PG_RECONNECT))
      .setSslMode(SslMode.of(config.getString("sslMode", EnvVars.SSL_MODE)))
      .setReconnectInterval(config.getInteger("pgReConnectInterval", EnvVars.PG_CONNECT_INTERVAL))
      .setHost(config.getString(Constants.PG_HOST, EnvVars.PG_HOST))
      .setDatabase(config.getString(Constants.PG_DATABASE, EnvVars.PG_DATABASE))
      .setUser(config.getString(Constants.PG_USER, EnvVars.PG_USER))
      .setPassword(config.getString(Constants.PG_PASSWORD, EnvVars.PG_PASSWORD))
      .setPort(config.getInteger("pgPort", EnvVars.PG_PORT))
      .addProperty("search_path", config.getString("schema", EnvVars.SCHEMA))
      .addProperty("application_name", config.getString("schema", EnvVars.SCHEMA));
  }

  public static RepositoryHandler leasePool(JsonObject configuration, Vertx vertx, Class<?> aggregate) {
    configuration.put(Constants.SCHEMA, camelToSnake(aggregate.getSimpleName()));
    return leasePool(configuration, vertx);
  }

  public static String camelToSnake(String str) {
    // Regular Expression
    String regex = "([a-z])([A-Z]+)";

    // Replacement string
    String replacement = "$1_$2";

    // Replace the given regex
    // with replacement string
    // and convert it to lower case.
    str = str
      .replaceAll(
        regex, replacement)
      .toLowerCase();

    // return string
    return str;
  }

  public Uni<Void> close() {
    return pgPool.close().flatMap(aVoid -> sqlClient.close());
  }

  public Function<Supplier<Uni<RowSet<Row>>>, Uni<Integer>> handleUpdate(Class<?> tClass) {
    final var start = Instant.now();
    return upstreamSupplier -> upstreamSupplier.get()
      .map(row -> {
          final var end = Instant.now();
          logger.info(tClass.getSimpleName() + " updated in " + Duration.between(start, end).toMillis() + "ms");
          return row != null && row.iterator().hasNext() ? row.iterator().next().getInteger(VERSION) : null;
        }
      ).onItem().ifNull()
      .failWith(
        new Conflict(
          new SqlError(
            null,
            tClass.getSimpleName() + " version mismatch conflict ! unable to updated record",
            null,
            null
          )

        )
      )
      .onFailure(throwable -> checkError(throwable, tClass))
      .retry().withBackOff(Duration.ofMillis(configuration.getInteger("repositoryRetryBackOff", EnvVars.REPOSITORY_RETRY_BACKOFF))).atMost(configuration.getInteger("repositoryMaxRetry", EnvVars.REPOSITORY_MAX_RETRY))
      .onFailure().transform(this::mapError);
  }

  public <T> Function<Supplier<Uni<RowSet<T>>>, Uni<T>> handleUpdateByKey(Class<T> tClass) {
    final var start = Instant.now();
    return upstreamSupplier -> upstreamSupplier.get()
      .map(Unchecked.function(row -> {
            final var end = Instant.now();
            logger.info(tClass.getSimpleName() + " fetched in " + Duration.between(start, end).toMillis() + "ms");
            if (row != null && row.iterator().hasNext()) {
              if (row.rowCount() > 1) {
                final var conflictingObjects = new ArrayList<T>();
                row.iterator().forEachRemaining(conflictingObjects::add);
                throw IntegrityContraintViolation.violation(tClass, conflictingObjects);
              }
              return row.iterator().next();
            }
            throw NotFound.notFound(tClass);
          }
        )
      )
      .onFailure(throwable -> checkError(throwable, tClass))
      .retry().withBackOff(Duration.ofMillis(configuration.getInteger("repositoryRetryBackOff", EnvVars.REPOSITORY_RETRY_BACKOFF))).atMost(configuration.getInteger("repositoryMaxRetry", EnvVars.REPOSITORY_MAX_RETRY))
      .onFailure().transform(this::mapError);
  }

  public <T> Function<Supplier<Uni<RowSet<T>>>, Uni<List<T>>> handleUpdateByKeyBatch(Class<T> tClass, int size) {
    final var start = Instant.now();
    return upstreamSupplier -> upstreamSupplier.get()
      .onItem().transformToMulti(RowSet::toMulti).collect().asList()
      .map(Unchecked.function(list -> {
            final var end = Instant.now();
            logger.info("Fetched results in " + Duration.between(start, end).toMillis() + "ms");
            if (size != list.size()) {
              throw new Conflict(new SqlError(
                "Size mismatch, should be " + size + " but is " + list.size(),
                "",
                null,
                null
              ));
            }
            return list;
          }
        )
      )
      .onFailure(throwable -> checkError(throwable, tClass))
      .retry().withBackOff(Duration.ofMillis(configuration.getInteger("repositoryRetryBackOff", EnvVars.REPOSITORY_RETRY_BACKOFF))).atMost(configuration.getInteger("repositoryMaxRetry", EnvVars.REPOSITORY_MAX_RETRY))
      .onFailure().transform(this::mapError);
  }

  private Throwable mapError(final Throwable throwable) {
    if (throwable instanceof PgException pgException) {
      if (pgException.getCode().startsWith("22")) {
//        Class 22 — Data Exception
        return new DataException(pgException);
      }
      if (pgException.getCode().startsWith("23")) {
//        Class 23 — Integrity Constraint Violation
        return new IntegrityContraintViolation(pgException);
      } else if (pgException.getCode().startsWith("5") || pgException.getCode().startsWith("08")) {
//        Class 53 — Insufficient Resources
//        Class 54 — Program Limit Exceeded
//        Class 55 — Object Not In Prerequisite State
//        Class 57 — Operator Intervention
//        Class 58 — System Error (errors external to PostgreSQL itself)
//        Class 08 — Connection Exception
        return new ConnectionFailure(pgException);
      } else {
        return new GenericFailure(pgException);
      }
    } else if (throwable instanceof ConnectException connectException) {
      return new ConnectionFailure(connectException);
    } else {
      return throwable;
    }
  }

  public Function<Supplier<Uni<RowSet<Row>>>, Uni<Long>> handleInsert(Object object) {
    final var start = Instant.now();
    return upstreamSupplier -> upstreamSupplier.get()
      .map(row -> {
          final var end = Instant.now();
          logger.info(" Inserted  " + row.rowCount() + " record in " + Duration.between(start, end).toMillis() + "ms");
//          return row != null && row.iterator().hasNext() ? row.iterator().next().getLong(ID) : null;
          return row.rowCount() == 0 ? null : (long) row.rowCount();
        }
      ).onItem().ifNull()
      .failWith(IntegrityContraintViolation.violation(object.getClass(), object))
      .onFailure(throwable -> checkError(throwable, object.getClass()))
      .retry().withBackOff(Duration.ofMillis(configuration.getInteger("repositoryRetryBackOff", EnvVars.REPOSITORY_RETRY_BACKOFF))).atMost(configuration.getInteger("repositoryMaxRetry", EnvVars.REPOSITORY_MAX_RETRY))
      .onFailure().transform(this::mapError);
  }

  public <T> Function<Supplier<Uni<RowSet<T>>>, Uni<List<T>>> handleInsertBatch(Class<T> tClass, int size) {
    final var start = Instant.now();
    return upstreamSupplier -> upstreamSupplier.get()
      .onItem().transformToMulti(RowSet::toMulti).collect().asList()
      .map(Unchecked.function(list -> {
            logger.debug("Fetched results -> " + list);
            final var end = Instant.now();
            if (size != list.size()) {
              throw new Conflict(
                new SqlError(
                  "Size mismatch, should be " + size + " but is " + list.size(),
                  "",
                  null,
                  null
                )
              );
            }
            logger.info(tClass.getSimpleName() + " query fetched " + list.size() + " in " + Duration.between(start, end).toMillis() + "ms");
            return list;
          }
        )
      )
      .onFailure(throwable -> checkError(throwable, tClass))
      .retry().withBackOff(Duration.ofMillis(configuration.getInteger("repositoryRetryBackOff", EnvVars.REPOSITORY_RETRY_BACKOFF))).atMost(configuration.getInteger("repositoryMaxRetry", EnvVars.REPOSITORY_MAX_RETRY))
      .onFailure().transform(this::mapError);
  }

  public Function<Supplier<Uni<RowSet<Row>>>, Uni<Long>> handleDelete(Class<?> tClass) {
    logger.debug("Handling delete query for " + tClass.getSimpleName());
    final var start = Instant.now();
    return upstreamSupplier -> upstreamSupplier.get()
      .map(row -> {
        final var end = Instant.now();
        logger.info(tClass.getSimpleName() + " deleted in " + Duration.between(start, end).toMillis() + "ms");
        return row.rowCount() == 0 ? null : (long) row.rowCount();
      }).onItem().ifNull()
      .failWith(NotFound.notFound(tClass))
      .onFailure(throwable -> checkError(throwable, tClass))
      .retry().withBackOff(Duration.ofMillis(configuration.getInteger("repositoryRetryBackOff", EnvVars.REPOSITORY_RETRY_BACKOFF))).atMost(configuration.getInteger("repositoryMaxRetry", EnvVars.REPOSITORY_MAX_RETRY))
      .onFailure().transform(this::mapError);
  }

  public <T> Function<Supplier<Uni<RowSet<T>>>, Uni<T>> handleSelectUnique(Class<T> tClass, Logger logger) {
    logger.debug("Handling select query for " + tClass.getSimpleName());
    final var start = Instant.now();
    return upstreamSupplier -> upstreamSupplier.get()
      .map(Unchecked.function(row -> {
            final var end = Instant.now();
            logger.info(tClass.getSimpleName() + " fetched in " + Duration.between(start, end).toMillis() + "ms");
            if (row != null && row.iterator().hasNext()) {
              if (row.rowCount() > 1) {
                final var conflictingObjects = new ArrayList<T>();
                row.iterator().forEachRemaining(conflictingObjects::add);
                throw IntegrityContraintViolation.violation(tClass, conflictingObjects);
              }
              return row.iterator().next();
            }
            throw NotFound.notFound(tClass);
          }
        )
      )
      .onFailure(throwable -> checkError(throwable, tClass))
      .retry().withBackOff(Duration.ofMillis(configuration.getInteger("repositoryRetryBackOff", EnvVars.REPOSITORY_RETRY_BACKOFF))).atMost(configuration.getInteger("repositoryMaxRetry", EnvVars.REPOSITORY_MAX_RETRY))
      .onFailure().transform(this::mapError);
  }


  public <T> Function<Supplier<Uni<RowSet<T>>>, Uni<Void>> handleExists(Class<T> tClass) {
    logger.debug("Handling exists query for " + tClass.getSimpleName());
    final var start = Instant.now();
    return upstreamSupplier -> upstreamSupplier.get()
      .map(rowSet -> {
        final var end = Instant.now();
        logger.info(tClass.getSimpleName() + " fetched in " + Duration.between(start, end).toMillis() + "ms");
        return rowSet.iterator().hasNext() ? null : rowSet;
      }).onItem().ifNull().failWith(GenericFailure.duplicated(tClass))
      .onFailure(throwable -> checkError(throwable, tClass))
      .retry().withBackOff(Duration.ofMillis(configuration.getInteger("repositoryRetryBackOff", EnvVars.REPOSITORY_RETRY_BACKOFF))).atMost(configuration.getInteger("repositoryMaxRetry", EnvVars.REPOSITORY_MAX_RETRY))
      .onFailure().transform(this::mapError)
      .replaceWithVoid();
  }

  public <T> Function<Supplier<Uni<RowSet<T>>>, Uni<List<T>>> handleQuery(Class<T> tClass) {
    logger.debug("Handling selectQ query for " + tClass.getSimpleName());
    final var start = Instant.now();
    return upstreamSupplier -> upstreamSupplier.get()
      .onItem().transformToMulti(RowSet::toMulti).collect().asList()
      .map(list -> {
          final var end = Instant.now();
          final var numberOfRecordsFetched = list == null ? 0 : list.size();
          logger.info(tClass.getSimpleName() + " query fetched " + numberOfRecordsFetched + " in " + Duration.between(start, end).toMillis() + "ms");
          return list == null || list.isEmpty() ? null : list;
        }
      )
      .onItem().ifNull().failWith(NotFound.notFound(tClass))
      .onFailure(throwable -> checkError(throwable, tClass))
      .retry().withBackOff(Duration.ofMillis(configuration.getInteger("repositoryRetryBackOff", EnvVars.REPOSITORY_RETRY_BACKOFF))).atMost(configuration.getInteger("repositoryMaxRetry", EnvVars.REPOSITORY_MAX_RETRY))
      .onFailure().transform(this::mapError);
  }


  private boolean checkError(Throwable throwable, final Class<?> tClass) {
    if (throwable instanceof final PgException pgException) {
      boolean b = pgException.getCode().startsWith("5") || pgException.getCode().startsWith("08");
      if (b) {
        logger.debug("Recoverable failure handling type for" + tClass + " , repository will retry", pgException);
      } else {
        logger.error("Unrecoverable failure", pgException);
      }
      return b;
    }
    return false;
  }

  public <T> Function<Supplier<Uni<RowSet<T>>>, Multi<T>> handleQueryMultiStream(Class<T> tClass) {
    logger.debug("Handling multi select query for " + tClass.getSimpleName());
    return upstreamSupplier -> upstreamSupplier.get()
      .onItem().transformToMulti(RowSet::toMulti)
      .onFailure(throwable -> checkError(throwable, tClass))
      .retry().withBackOff(Duration.ofMillis(configuration.getInteger("repositoryRetryBackOff", EnvVars.REPOSITORY_RETRY_BACKOFF)))
      .atMost(configuration.getInteger("repositoryMaxRetry", EnvVars.REPOSITORY_MAX_RETRY))
      .onFailure().transform(this::mapError);
  }

  public <V> Uni<Void> handleStreamProcessing(PgPool pgPool, Lock lock, String statement, RowMapper<V> rowMapper, Consumer<V> vConsumer) {
    logger.debug("Handling stream query :" + statement);
    return pgPool.getConnection()
      .onFailure().transform(
        Unchecked.function(throwable -> {
          logger.error(rowMapper.getClass().getSimpleName() + " row streamer failed to obtain connection", throwable);
          releaseLock(lock);
          throw new GenericFailure(new SqlError(
            "Connection timeout",
            "Row streamer could not obtain connection for type :" + statement,
            null,
            null
          )
          );
        })
      )
      .invoke(sqlConnection -> logger.debug("Row streamer obtained connection"))
      .flatMap(
        sqlConnection -> sqlConnection.begin()
          .onFailure().invoke(throwable -> {
            logger.error("row streamer failed to start transaction", throwable);
            releaseLock(lock);
            sqlConnection.closeAndForget();
          })
          .invoke(transaction -> logger.debug("Row streamer started transaction"))
          .flatMap(
            transaction -> sqlConnection.prepare(statement)
              .onFailure().invoke(throwable -> {
                  logger.error("row streamer failed to start transaction", throwable);
                  releaseLock(lock);
                  transaction.commitAndForget();
                  transaction.completionAndForget();
                  sqlConnection.closeAndForget();
                }
              )
              .invoke(
                preparedStatement -> {
                  final var stream = preparedStatement.createStream(configuration.getInteger("repositoryStreamBatchSize", EnvVars.REPOSITORY_STREAM_BATCH_SIZE));
                  stream.fetch(EnvVars.REPOSITORY_STREAM_BATCH_SIZE)
                    .handler(row -> {
                        logger.debug("Stream fetched " + row.toJson().encodePrettily());
                        vConsumer.accept(rowMapper.map(row));
                      }
                    )
                    .exceptionHandler(
                      throwable -> {
                        logger.error("Exception during row streaming ", throwable);
                      }
                    )
                    .endHandler(
                      () -> {
                        logger.info("Closing stream....");
                        closeStream(stream, lock, transaction, preparedStatement, sqlConnection);
                      }
                    );
                }
              )
          )
      )
      .replaceWithVoid();
  }

  public <V> Uni<Void> handleStreamProcessing(PgPool pgPool, Lock lock, String statement, RowMapper<V> rowMapper, Consumer<V> vConsumer, Tuple arguments, Integer batchSize) {
    logger.debug("Handling stream query :" + statement);
    return pgPool.getConnection()
      .onFailure().transform(Unchecked.function(throwable -> {
          logger.error(rowMapper.getClass().getSimpleName() + " row streamer failed to obtain connection", throwable);
          releaseLock(lock);
          throw new GenericFailure(new SqlError(
            "Connection timeout",
            "Row streamer could not obtain connection for type :" + statement,
            null,
            null
          )
          );
        })
      )
      .invoke(sqlConnection -> logger.debug("Row streamer obtained connection"))
      .flatMap(
        sqlConnection -> sqlConnection.begin()
          .onFailure().invoke(throwable -> {
            logger.error("row streamer failed to start transaction", throwable);
            releaseLock(lock);
            sqlConnection.closeAndForget();
          })
          .invoke(transaction -> logger.debug("Row streamer started transaction"))
          .flatMap(
            transaction -> sqlConnection.prepare(statement)
              .onFailure().invoke(throwable -> {
                  logger.error("row streamer failed to start transaction", throwable);
                  releaseLock(lock);
                  transaction.commitAndForget();
                  transaction.completionAndForget();
                  sqlConnection.closeAndForget();
                }
              )
              .invoke(
                preparedStatement -> {
                  final var stream = preparedStatement.createStream(batchSize, arguments);
                  stream.handler(row -> {
                        logger.debug("Stream fetched " + row.toJson().encodePrettily());
                        vConsumer.accept(rowMapper.map(row));
                      }
                    )
                    .exceptionHandler(throwable -> logger.error("Exception during row streaming ", throwable))
                    .endHandler(
                      () -> {
                        logger.info("Closing stream....");
                        closeStream(stream, lock, transaction, preparedStatement, sqlConnection);
                      }
                    );
                }
              )
          )
      )
      .replaceWithVoid();
  }

  private void releaseLock(final Lock lock) {
    if (lock != null) {
      lock.release();
      logger.info("Lock Released :" + lock);
    }
  }

  public <V> Uni<Void> handleStreamProcessing(PgPool pgPool, Uni<Void> lock, String statement, RowMapper<V> rowMapper, Consumer<V> vConsumer, Tuple arguments) {
    logger.debug("Handling stream query :" + statement);
    return pgPool.getConnection()
      .onFailure().call(Unchecked.function(throwable -> {
            logger.error(rowMapper.getClass().getSimpleName() + " row streamer failed to obtain connection", throwable);
            if (lock != null) {
              return lock.onItemOrFailure()
                .transformToUni(Unchecked.function((item, throwable1) -> {
                      throw new GenericFailure(new SqlError(
                        "Connection timeout",
                        "Row streamer could not obtain connection for type :" + statement,
                        null,
                        null
                      )
                      );
                    }
                  )
                );
            } else {
              throw new GenericFailure(new SqlError(
                "Connection timeout",
                "Row streamer could not obtain connection for type :" + statement,
                null,
                null
              )
              );
            }
          }
        )
      )
      .invoke(sqlConnection -> logger.debug("Row streamer obtained connection"))
      .flatMap(sqlConnection -> sqlConnection.begin()
        .onFailure().call(throwable -> {
            logger.error("row streamer failed to start transaction", throwable);
            if (lock != null) {
              return sqlConnection.close()
                .call(aVoid -> lock)
                .onItemOrFailure()
                .call(Unchecked.function((item, throwable1) -> {
                      throw new GenericFailure(
                        new SqlError(
                          "Unable to obtain connection",
                          "Row streamer could not obtain connection for type :" + statement,
                          "Row streamer could not obtain connection for type :" + statement,
                          null
                        )
                      );
                    }
                  )
                );
            } else {
              return sqlConnection.close()
                .onItemOrFailure()
                .call(Unchecked.function((item, throwable1) -> {
                      throw new GenericFailure(new SqlError(
                        "Connection timeout",
                        "Row streamer could not obtain connection for type :" + statement,
                        null,
                        null)
                      );
                    }
                  )
                );
            }
          }
        )
        .invoke(transaction -> logger.debug("Row streamer started transaction"))
        .flatMap(transaction -> {
            final var promise = Promise.promise();
            return sqlConnection.prepare(statement)
              .onFailure().call(throwable -> {
                  logger.error("row streamer failed to start transaction", throwable);
                  if (lock != null) {
                    return transaction.commit()
                      .call(aVoid -> transaction.completion())
                      .call(aVoid -> sqlConnection.close())
                      .call(aVoid -> lock)
                      .onItemOrFailure()
                      .transform(Unchecked.function((item, throwable1) -> {
                            throw new GenericFailure(new SqlError(
                              "Connection timeout",
                              "Row streamer could not obtain connection for statement :" + statement,
                              null,
                              null
                            )
                            );
                          }
                        )
                      );
                  } else {
                    return transaction.commit()
                      .call(aVoid -> transaction.completion())
                      .call(aVoid -> sqlConnection.close())
                      .onItemOrFailure()
                      .call(Unchecked.function((item, throwable1) -> {
                            throw new GenericFailure(new SqlError(
                              "Connection timeout",
                              "Row streamer could not obtain connection for type :" + statement,
                              null,
                              null
                            )
                            );
                          }
                        )
                      );
                  }
                }
              )
              .invoke(
                preparedStatement -> {
                  final var stream = preparedStatement.createStream(configuration.getInteger("repositoryStreamBatchSize", EnvVars.REPOSITORY_STREAM_BATCH_SIZE), arguments);
                  stream.fetch(EnvVars.REPOSITORY_STREAM_BATCH_SIZE)
                    .handler(row -> {
                        logger.debug("Stream fetched " + row.toJson().encodePrettily());
                        vConsumer.accept(rowMapper.map(row));
                      }
                    )
                    .exceptionHandler(throwable -> logger.error("Exception during row streaming ", throwable))
                    .endHandler(
                      () -> {
                        logger.info("Closing stream....");
                        closeStream(stream, lock, transaction, preparedStatement, sqlConnection);
                        promise.complete();
                      }
                    );
                }
              )
              .call(avoid -> promise.future());
          }
        )
      )
      .replaceWithVoid();
  }

  private void closeStream(RowStream<Row> stream, Lock lock, Transaction transaction, PreparedStatement preparedStatement, SqlConnection sqlConnection) {
    stream.close()
      .call(v -> transaction.commit())
      .call(v -> transaction.completion())
      .call(v -> preparedStatement.close())
      .call(v -> sqlConnection.close())
      .invoke(v -> releaseLock(lock))
      .subscribe()
      .with(
        v -> logger.info("Stream closed nicely....")
        , t -> logger.error("Error closing the stream", t)
      );
  }

  private void closeStream(RowStream<Row> stream, Uni<Void> lock, Transaction transaction, PreparedStatement preparedStatement, SqlConnection sqlConnection) {
    lock.call(aVoid -> stream.close())
      .call(aVoid -> transaction.commit())
      .call(aVoid -> transaction.completion())
      .call(aVoid -> preparedStatement.close())
      .call(aVoid -> sqlConnection.close())
      .subscribe()
      .with(
        v -> logger.info("Stream closed nicely....")
        , t -> logger.error("Error closing the stream", t)
      );
  }


}
