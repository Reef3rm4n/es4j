package io.es4j.domain;

import com.google.auto.service.AutoService;
import io.es4j.Aggregate;
import io.es4j.Deployment;


@AutoService(Deployment.class)
public class Bootstrapper implements Deployment {
  @Override
  public Class<? extends Aggregate> aggregateClass() {
    return FakeAggregate.class;
  }
}
