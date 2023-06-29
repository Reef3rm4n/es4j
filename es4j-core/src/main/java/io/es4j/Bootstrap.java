package io.es4j;

import java.util.Collections;
import java.util.List;

public interface Bootstrap {

  Class<? extends Aggregate> aggregateClass();

  default List<String> fileConfigurations() {
    return Collections.emptyList();
  }

}
