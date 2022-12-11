package io.vertx.skeleton.models;


import io.vertx.core.shareddata.Shareable;

import java.io.Serializable;
import java.util.Objects;


public record Tenant(Integer brandId, Integer partnerId) implements Shareable, Serializable {

  public Tenant {
    Objects.requireNonNull(brandId);
    Objects.requireNonNull(partnerId);
  }


  public static Tenant from(String brandId, String partnerId) {
    return new Tenant(Integer.valueOf(brandId), Integer.valueOf(partnerId));
  }

  public static Tenant fromString(String tenant) {
    final var split = tenant.split("::");
    return new Tenant(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
  }

  public static Tenant unknown() {
    return new Tenant(- 666, - 666);
  }

  public String generateString() {
    return brandId + "::" + partnerId;
  }


}
