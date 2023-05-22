package io.eventx.config.exceptions;

import io.vertx.core.json.JsonObject;


public class ConfigStoreException extends RuntimeException {

  private final ConfigStoreError configStoreError;

  public ConfigStoreException(ConfigStoreError configStoreError) {
    super(JsonObject.mapFrom(configStoreError).encodePrettily());
    this.configStoreError = configStoreError;
  }

  public ConfigStoreException(Throwable throwable) {
    super(throwable.getMessage(), throwable);
    this.configStoreError = new ConfigStoreError(
      throwable.getMessage(),
      throwable.getLocalizedMessage(),
      500
    );
  }
  public ConfigStoreException(ConfigStoreError configStoreError, Throwable throwable) {
    super(configStoreError.cause(), throwable);
    this.configStoreError = configStoreError;
  }

  public ConfigStoreException(String cause, String hint, Integer errorCode) {
    this.configStoreError = new ConfigStoreError(cause, hint, errorCode);
  }

  public ConfigStoreException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, throwable);
    this.configStoreError = new ConfigStoreError(cause, hint, errorCode);
  }

  public ConfigStoreError error() {
    return configStoreError;
  }


}
