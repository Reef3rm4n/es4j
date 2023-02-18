package io.vertx.skeleton.models.exceptions;


import io.vertx.skeleton.models.Error;

public class CacheException extends VertxServiceException {

  public CacheException(Error error) {
    super(error);
  }

  public static CacheException illegalState() {
    return new CacheException(new Error("Illegal state", "", 500));
  }


  public static CacheException notImplemented() {
    return new CacheException(new Error("Not Implemented", "", 500));
  }


  public static CacheException notImplemented(String hint) {
    return new CacheException(new Error("Not Implemented", hint, 500));
  }

  public static CacheException notFound(Object key) {
    return new CacheException(new Error("Not Found", "Key not found "+key.toString(), 500));
  }


}
