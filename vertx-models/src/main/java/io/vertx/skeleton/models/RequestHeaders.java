package io.vertx.skeleton.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.ws.rs.HeaderParam;
import java.io.Serializable;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RequestHeaders(
  @HeaderParam(REQUEST_ID) String requestID,
  @HeaderParam(TENANT_ID) String tenantId,
  @HeaderParam(USER_ID) String userId,
  @HeaderParam(TOKEN) String token
) implements Serializable {
  public static final String REQUEST_ID = "REQ-ID";
  public static final String USER_ID = "USER-ID";
  public static final String TOKEN = "TOKEN-ID";
  public static final String TENANT_ID = "TENANT-ID";

  public static RequestHeaders from(String tenantID, String transactionId) {
    return new RequestHeaders(
      transactionId,
      tenantID,
      null,
      null
    );
  }

  public static RequestHeaders from(String transactionId) {
    return new RequestHeaders(
      transactionId,
      "default",
      null,
      null
    );
  }

  public static RequestHeaders defaultHeaders() {
    return new RequestHeaders(
      UUID.randomUUID().toString(),
      "default",
      null,
      null
    );
  }


}
