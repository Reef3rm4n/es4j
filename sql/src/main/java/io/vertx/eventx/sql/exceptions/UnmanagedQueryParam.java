package io.vertx.eventx.sql.exceptions;


import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventxException;

import java.util.Collection;
import java.util.List;

public class UnmanagedQueryParam extends EventxException {
  public UnmanagedQueryParam(EventXError eventxError) {
    super(eventxError);
  }

  public static UnmanagedQueryParam unmanagedParams(List<?> item2) {
    return new UnmanagedQueryParam(new EventXError("Query param contained a non managed type -> " + item2.stream().findAny().map(object -> object.getClass().getName()).orElseThrow(),"",400));
  }

  public static UnmanagedQueryParam unmanagedParams(Collection<?> item2) {
    return new UnmanagedQueryParam(new EventXError("Query param contained a non managed type -> " + item2.stream().findAny().map(object -> object.getClass().getName()).orElseThrow(),"",400));
  }
}
