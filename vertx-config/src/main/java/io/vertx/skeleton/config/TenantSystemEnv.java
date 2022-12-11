package io.vertx.skeleton.config;

import io.vertx.skeleton.models.Tenant;

public class TenantSystemEnv {


  public static String getTenantEnvVar(Tenant tenant, String varName) {
    return System.getenv().get(tenant.brandId() + "_" + tenant.partnerId() + "_" + varName);
  }
}
