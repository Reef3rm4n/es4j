package io.eventx;

public interface Bootstrap {

  Class<? extends Aggregate> aggregateClass();
}
