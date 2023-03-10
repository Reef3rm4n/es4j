package io.vertx.eventx.sql.exceptions;


import io.vertx.eventx.sql.misc.SqlError;

import java.util.List;

public class UnmanagedQueryParam extends SqlException {
  public UnmanagedQueryParam(SqlError eventxError) {
    super(eventxError);
  }

  public static UnmanagedQueryParam unmanagedParams(List<?> item2) {
    return new UnmanagedQueryParam(new SqlError(
      "Query param contained a non managed type -> " + item2.stream().findAny().map(object -> object.getClass().getName()).orElseThrow(), "" +
      "",
      null,
      null
    )
    );
  }
}
