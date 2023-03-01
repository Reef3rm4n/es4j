

package io.vertx.eventx.common;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface KeyValue<K,V> {


  K getKey();

  @JsonIgnore
  V getValue();

}
