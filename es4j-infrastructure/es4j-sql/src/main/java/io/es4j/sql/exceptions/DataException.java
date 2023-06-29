package io.es4j.sql.exceptions;


import io.es4j.sql.misc.SqlError;
import io.vertx.pgclient.PgException;

public class DataException extends SqlException {


  public DataException(SqlError error) {
    super(error);
  }

  public DataException(Throwable throwable) {
    super(throwable);
  }

  public DataException(PgException pgException) {
    super(pgException);
  }
}
