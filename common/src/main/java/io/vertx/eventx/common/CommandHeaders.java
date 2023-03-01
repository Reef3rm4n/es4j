package io.vertx.eventx.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.ws.rs.HeaderParam;
import java.io.Serializable;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CommandHeaders(
  @HeaderParam(COMMAND_ID) String commandID,
  @HeaderParam(TENANT_ID) String tenantId,
  @HeaderParam(TOKEN) String token,
  @HeaderParam(COMMAND_CLASS) String commandClass
) implements Serializable {
  public static final String COMMAND_ID = "COMMAND-ID";
  public static final String TOKEN = "TOKEN-ID";
  public static final String TENANT_ID = "TENANT-ID";
  public static final String COMMAND_CLASS = "COMMAND-CLASS";

  public static CommandHeaders simple(String tenantID, String commandID) {
    return new CommandHeaders(
      commandID,
      tenantID,
      null,
      null
    );
  }

  public static CommandHeaders simple(String commandID) {
    return new CommandHeaders(
      commandID,
      "default",
      null,
      null
    );
  }

  public static CommandHeaders defaultHeaders() {
    return new CommandHeaders(
      UUID.randomUUID().toString(),
      "default",
      null,
      null
    );
  }


}
