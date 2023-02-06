package io.vertx.skeleton.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.ws.rs.HeaderParam;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RequestHeaders(
  @HeaderParam(TRANSACTION_ID) String transactionId,
  @HeaderParam(TENANT_ID_HEADER) String tenantId,
  @HeaderParam(USER_ID) String userId,
  @HeaderParam(TOKEN) String token
) implements Serializable {
  public static final String TRANSACTION_ID = "X-TXT-ID";
  public static final String USER_ID = "USER-ID";
  public static final String TOKEN = "TOKEN-ID";
  public static final String TENANT_ID_HEADER = "TENANT-ID";

  public TenantV2 tenant() {
    return new TenantV2(tenantId);
  }

  public static RequestHeaders from(TenantV2 tenant, String transactionId) {
    return new RequestHeaders(
      transactionId,
      tenant.id(),
      null,
      null
    );
  }


}
