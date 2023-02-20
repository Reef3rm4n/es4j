package io.vertx.skeleton.sql.exceptions;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.VertxServiceException;

import java.util.Collection;
import java.util.List;

public class UnmanagedQueryParamException extends VertxServiceException {
  public UnmanagedQueryParamException(Error error) {
    super(error);
  }

  public static UnmanagedQueryParamException unmanagedParams(List<?> item2) {
    return new UnmanagedQueryParamException(new Error("Query param contained a non managed type -> " + item2.stream().findAny().map(object -> object.getClass().getName()).orElseThrow(),"",400));
  }

  public static UnmanagedQueryParamException unmanagedParams(Collection<?> item2) {
    return new UnmanagedQueryParamException(new Error("Query param contained a non managed type -> " + item2.stream().findAny().map(object -> object.getClass().getName()).orElseThrow(),"",400));
  }
}
