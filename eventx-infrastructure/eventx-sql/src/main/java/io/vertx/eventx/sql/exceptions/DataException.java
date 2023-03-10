package io.vertx.eventx.sql.exceptions;


import io.vertx.eventx.sql.misc.SqlError;
import io.vertx.pgclient.PgException;

import java.util.List;

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
