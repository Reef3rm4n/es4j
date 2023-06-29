package io.es4j.sql.exceptions;


import io.vertx.pgclient.PgException;

public class ConnectionFailure extends SqlException {



  public ConnectionFailure(Throwable throwable) {
    super(throwable);
  }

  public ConnectionFailure(PgException pgException) {
    super(pgException);
  }
}
