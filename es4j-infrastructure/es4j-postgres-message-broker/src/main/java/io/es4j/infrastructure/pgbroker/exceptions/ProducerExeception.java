package io.es4j.infrastructure.pgbroker.exceptions;



public class ProducerExeception extends RuntimeException {

  public ProducerExeception(Throwable throwable) {
    super(throwable);
  }

    public ProducerExeception(String throwable) {
        super(throwable);
    }

}
