package io.vertx.eventx.sql.exceptions;


import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventxException;

public class GenericError extends EventxException {


  public GenericError(EventXError cobraEventXError) {
    super(cobraEventXError);
  }


  public static GenericError illegalState() {
    return new GenericError(new EventXError("Illegal state", "", 500));
  }


  public static GenericError notImplemented() {
    return new GenericError(new EventXError("Not Implemented", "", 500));
  }


  public static GenericError notImplemented(String hint) {
    return new GenericError(new EventXError("Not Implemented", hint, 500));
  }

  public static <T> GenericError notFound(Class<T> tClass) {
    return new GenericError(new EventXError(tClass.getSimpleName() + " not found !", "Check query parameters", 400));
  }

  public static <T> GenericError duplicated(Class<T> tClass) {
    return new GenericError(new EventXError("Duplicated record " + tClass.getSimpleName(), null, 400));
  }
}
