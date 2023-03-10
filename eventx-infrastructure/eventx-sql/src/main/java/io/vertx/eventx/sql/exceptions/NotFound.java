package io.vertx.eventx.sql.exceptions;


import io.vertx.eventx.sql.misc.SqlError;

public class NotFound extends SqlException {
  public NotFound(SqlError cobraEventxError) {
    super(cobraEventxError);
  }

  public static <T> NotFound notFound(Class<T> tClass) {
    return new NotFound(new SqlError(
      "interrupting stream",
      "irrelevant",
      tClass.getSimpleName() + "Data not found ",
      ""
    )
    );
  }

}
