package io.es4j.sql.exceptions;


import io.es4j.sql.misc.SqlError;
import io.vertx.pgclient.PgException;

public class Conflict extends SqlException {


  public Conflict(SqlError error) {
    super(error);
  }

  public Conflict(Throwable throwable) {
    super(throwable);
  }

  public Conflict(PgException pgException) {
    super(pgException);
  }

}
