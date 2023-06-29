package io.es4j.domain;

import com.google.auto.service.AutoService;
import io.es4j.Aggregate;
import io.es4j.Bootstrap;


@AutoService(Bootstrap.class)
public class Bootstrapper implements Bootstrap {
  @Override
  public Class<? extends Aggregate> aggregateClass() {
    return FakeAggregate.class;
  }
}
