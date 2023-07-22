package io.es4j.infrastructure.pgbroker.exceptions;

public class DuplicateMessage extends RuntimeException {
    public DuplicateMessage(Throwable e) {
        super(e);
    }
    public DuplicateMessage(String e) {
        super(e);
    }
}
