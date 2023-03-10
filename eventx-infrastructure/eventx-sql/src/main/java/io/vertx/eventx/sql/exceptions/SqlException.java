package io.vertx.eventx.sql.exceptions;

import io.vertx.core.json.JsonObject;
import io.vertx.eventx.sql.misc.SqlError;
import io.vertx.pgclient.PgException;

public class SqlException extends RuntimeException {
  private final SqlError sqlError;

  public SqlException(SqlError error) {
    super(JsonObject.mapFrom(error).encodePrettily());
    this.sqlError = error;
  }

  public SqlException(Throwable throwable) {
    super(throwable.getMessage(), throwable);
    this.sqlError = new SqlError(
      throwable.getMessage(),
      "FATAL",
      throwable.getLocalizedMessage(),
      null
    );
  }

  public SqlException(PgException pgException) {
    super(JsonObject.mapFrom(new SqlError(
      pgException.getErrorMessage(),
      pgException.getSeverity(),
      pgException.getCode(),
      pgException.getDetail()
    )).encodePrettily(), pgException);
    this.sqlError = new SqlError(
      pgException.getErrorMessage(),
      pgException.getSeverity(),
      pgException.getCode(),
      pgException.getDetail()
    );
  }


  public SqlError error() {
    return sqlError;
  }
}
