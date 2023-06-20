package io.eventx.domain;

import com.google.auto.service.AutoService;
import io.eventx.Aggregate;
import io.eventx.Bootstrap;


@AutoService(Bootstrap.class)
public class Bootstrapper implements Bootstrap {
  @Override
  public Class<? extends Aggregate> aggregateClass() {
    return FakeAggregate.class;
  }
}
