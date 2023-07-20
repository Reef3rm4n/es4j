package io.es4j.domain;

import com.google.auto.service.AutoService;
import io.es4j.Aggregate;
import io.es4j.Es4jDeployment;


@AutoService(Es4jDeployment.class)
public class Bootstrapper implements Es4jDeployment {
  @Override
  public Class<? extends Aggregate> aggregateClass() {
    return FakeAggregate.class;
  }
}
