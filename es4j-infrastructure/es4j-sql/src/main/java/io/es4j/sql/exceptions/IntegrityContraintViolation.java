package io.es4j.sql.exceptions;


import io.es4j.sql.misc.SqlError;
import io.vertx.pgclient.PgException;

public class IntegrityContraintViolation extends SqlException {


  public IntegrityContraintViolation(SqlError error) {
    super(error);
  }

  public IntegrityContraintViolation(Throwable throwable) {
    super(throwable);
  }

  public IntegrityContraintViolation(PgException pgException) {
    super(pgException);
  }

  public static <T> IntegrityContraintViolation violation(Class<T> tClass, Object object) {
    return new IntegrityContraintViolation(new SqlError(
      tClass.getSimpleName() + " violated integrity",
      tClass.getSimpleName() + " violated integrity",
      "Should be unique but found -> " + object,
      "Should be unique but found -> " + object
    )
    );
  }

}
