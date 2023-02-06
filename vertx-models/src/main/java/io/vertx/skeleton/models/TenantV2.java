package io.vertx.skeleton.models;


import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.util.Objects;


public record TenantV2(String id) implements Shareable, Serializable {

  public TenantV2 {
    Objects.requireNonNull(id);
  }


}
