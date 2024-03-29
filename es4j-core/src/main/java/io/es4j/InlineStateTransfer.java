package io.es4j;

import io.es4j.core.objects.AggregateState;
import io.smallrye.mutiny.Uni;

// create javadoc for this interface
public interface InlineStateTransfer<T extends Aggregate> {

  Uni<Void> update(AggregateState<T> currentState);

  default String tenant() {
    return "default";
  }


}
