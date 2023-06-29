package io.es4j.sql.exceptions;

import io.es4j.sql.misc.SqlError;
import io.vertx.pgclient.PgException;

public class GenericFailure extends SqlException {


  public GenericFailure(SqlError error) {
    super(error);
  }

  public GenericFailure(Throwable throwable) {
    super(throwable);
  }

  public GenericFailure(PgException pgException) {
    super(pgException);
  }

  public static GenericFailure notImplemented() {
    return new GenericFailure(new SqlError(
      "Not Implemented", "",
      null,
      null
    )
    );
  }

  public static <T> GenericFailure duplicated(Class<T> tClass) {
    return new GenericFailure(new SqlError("", "Duplicated record " + tClass.getSimpleName(), "", ""));
  }
}
