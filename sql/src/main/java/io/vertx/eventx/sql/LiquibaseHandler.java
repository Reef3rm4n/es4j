package io.vertx.eventx.sql;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.UniHelper;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.eventx.sql.misc.Constants;
import io.vertx.eventx.sql.misc.EnvVars;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.AbstractResourceAccessor;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.InputStreamList;
import org.apache.commons.text.StringSubstitutor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import static io.vertx.eventx.sql.misc.Constants.SCHEMA;


public class LiquibaseHandler {

  private LiquibaseHandler(){}

  private static final Logger logger = LoggerFactory.getLogger(LiquibaseHandler.class);
  private static final String REVISION = System.getenv().getOrDefault("REVISION", null);


  public static Uni<Void> handle(Vertx vertx, JsonObject config) {
    if (Boolean.TRUE.equals(EnvVars.LIQUIBASE)) {
      return runliquibaseChangeLogFile(vertx, config);
    }
    return Uni.createFrom().voidItem();
  }

  public static Uni<Void> runliquibaseChangeLogFile(Vertx vertx, JsonObject config) {
    return UniHelper.toUni(vertx.getDelegate().executeBlocking(
        promise -> {
          final var pgHost = config.getString(Constants.PG_HOST, EnvVars.PG_HOST);
          final var pgPassword = config.getString(Constants.PG_PASSWORD, EnvVars.PG_PASSWORD);
          final var pgUser = config.getString(Constants.PG_USER, EnvVars.PG_USER);
          final var pgDatabase = config.getString(Constants.PG_DATABASE, EnvVars.PG_DATABASE);
          final var pgPort = config.getString(Constants.PG_PORT, String.valueOf(EnvVars.PG_PORT));
          final var schema = config.getString(Constants.SCHEMA, EnvVars.SCHEMA);
          final var changelog = config.getString(Constants.CHANGELOG, EnvVars.CHANGELOG);
          final String url = config.getString(Constants.JDBC_URL, "jdbc:postgresql://" + pgHost + ":" + pgPort + "/" + pgDatabase);
          logger.debug("Using jdbc connection string -> " + url);
          final Connection conn = getProps(pgPassword, pgUser, schema, url);
          final Liquibase liquibase = liquibase(schema, changelog, conn);
          final var liquibaseTag = config.getString("liquibaseTag", REVISION);
          final var rollbackPoint = "rollbackPoint::" + liquibaseTag;
          final var context = config.getString("liquibaseContext");
          try {
            if (!liquibase.tagExists(liquibaseTag)) {
              placeTag(liquibase, rollbackPoint, promise);
              try {
                if (Boolean.TRUE.equals(config.getBoolean("liquibaseTestRollback", false))) {
                  liquibase.updateTestingRollback(liquibaseTag, new Contexts(context), null);
                }
                liquibase.update(liquibaseTag, context);
              } catch (LiquibaseException liquibaseException) {
                try {
                  liquibase.rollback(rollbackPoint, context);
                  promise.fail(liquibaseException);
                } catch (LiquibaseException liquibaseException1) {
                  promise.fail(liquibaseException1);
                }
                logger.error("Unable to update");
                promise.fail(liquibaseException);
              }
            }
            liquibase.close();
            conn.close();
          } catch (LiquibaseException e) {
            logger.error("Error handling liquibase", e);
            promise.fail(e);
          } catch (SQLException sqlException) {
            logger.error("Unable");
            promise.fail(sqlException);
          }
          promise.complete();
        }
      )
    );
  }

  public static Uni<Void> liquibaseString(
    RepositoryHandler repositoryHandler,
    String fileName,
    Map<String, String> params
  ) {
    return repositoryHandler.vertx().fileSystem().readFile(fileName)
      .flatMap(buffer -> runliquibaseChangeLogString(
        repositoryHandler.vertx(),
        repositoryHandler.configuration(),
        replacePlaceHolders(buffer.toString(), params),
        fileName
        )
      );
  }

  public static String replacePlaceHolders(String target, Map<String, String> params) {
    return new StringSubstitutor(params).replace(target);
  }


  public static Uni<Void> runliquibaseChangeLogString(Vertx vertx, JsonObject config, String changelog, String fileName) {
    return UniHelper.toUni(vertx.getDelegate().executeBlocking(
        promise -> {
          final var pgHost = config.getString(Constants.PG_HOST, EnvVars.PG_HOST);
          final var pgPassword = config.getString(Constants.PG_PASSWORD, EnvVars.PG_PASSWORD);
          final var pgUser = config.getString(Constants.PG_USER, EnvVars.PG_USER);
          final var pgDatabase = config.getString(Constants.PG_DATABASE, EnvVars.PG_DATABASE);
          final var pgPort = config.getString(Constants.PG_PORT, String.valueOf(EnvVars.PG_PORT));
          final var schema = config.getString(SCHEMA, EnvVars.SCHEMA);
          final var url = config.getString(Constants.JDBC_URL, "jdbc:postgresql://" + pgHost + ":" + pgPort + "/" + pgDatabase);
          logger.debug("Using jdbc connection string -> " + url);
          final Connection conn = getProps(pgPassword, pgUser, schema, url);
          final var liquibase = liquibaseForStringChangelog(schema, changelog, conn, fileName);
          try {
            liquibase.update(new Contexts());
            liquibase.close();
            conn.close();
          } catch (LiquibaseException e) {
            logger.error("Error handling liquibase", e);
            promise.fail(e);
          } catch (SQLException sqlException) {
            logger.error("Unable");
            promise.fail(sqlException);
          }
          promise.complete();
        }
      )
    );
  }

  private static class StringAccessor extends AbstractResourceAccessor {
    private String changelogXml;

    public StringAccessor(String changelogXml) {
      super();
      this.changelogXml = changelogXml;
    }

    @Override
    public SortedSet<String> list(String relativeTo, String path, boolean recursive, boolean includeFiles, boolean includeDirectories) throws IOException {
      SortedSet<String> returnSet = new TreeSet<>();
      returnSet.add(changelogXml);
      return returnSet;
    }


    @Override
    public InputStreamList openStreams(String relativeTo, String streamPath) throws IOException {
      return new InputStreamList();
    }

    @Override
    public InputStream openStream(String relativeTo, String streamPath) throws IOException {
      return new ByteArrayInputStream(changelogXml.getBytes());
    }

    @Override
    public SortedSet<String> describeLocations() {
      return new TreeSet<>();
    }


  }


  private static void placeTag(final Liquibase liquibase, final String tag, Promise<Void> promise) {
    try {
      if (liquibase.tagExists(tag)) {
        liquibase.tag(tag);
      }
    } catch (LiquibaseException liquibaseException) {
      logger.error("Unable to place tag " + tag, liquibaseException);
      promise.fail(liquibaseException);
    }
  }

  private static Liquibase liquibaseForStringChangelog(final String schema, final String changelog, final Connection conn, String fileName) {
    final Database database;
    final var resourceAccessor = new StringAccessor(changelog);
    try {
      database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn));
      database.setDefaultSchemaName(schema);
      database.setLiquibaseSchemaName(schema);
    } catch (DatabaseException e) {
      throw new IllegalArgumentException(e);
    }
    return new Liquibase(fileName, resourceAccessor, database);
  }

  private static Liquibase liquibase(final String schema, final String changelog, final Connection conn) {
    final Database database;
    try {
      database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn));
      database.setDefaultSchemaName(schema);
      database.setLiquibaseSchemaName(schema);
    } catch (DatabaseException e) {
      throw new IllegalArgumentException(e);
    }
    return new Liquibase(changelog, new ClassLoaderResourceAccessor(), database);
  }

  private static Connection getProps(final String pgPassword, final String pgUser, final String schema, final String url) {
    final var props = new Properties();
    props.setProperty("user", String.valueOf(pgUser));
    props.setProperty("password", pgPassword);
    props.setProperty("ssl", "false");
    Connection conn;
    try {
      conn = DriverManager.getConnection(url, props);
      conn.createStatement().execute("create schema if not exists " + schema + ";");
      conn.setSchema(schema);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
    return conn;
  }

}
