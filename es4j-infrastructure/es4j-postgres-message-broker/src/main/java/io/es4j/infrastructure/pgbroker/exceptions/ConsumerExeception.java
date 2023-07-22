package io.es4j.infrastructure.pgbroker.exceptions;


public class ConsumerExeception extends RuntimeException {

  public ConsumerExeception(String error) {
    super(error);
  }

  public ConsumerExeception(Throwable throwable) {
    super(throwable);
  }


}
