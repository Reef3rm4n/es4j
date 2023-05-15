package io.vertx.eventx.objects;



import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.io.Serializable;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
@RecordBuilder
public record CommandHeaders(
  String commandID,
  String tenantId,
  String token,
  String commandClass
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
  public static CommandHeaders defaultHeaders(String tenantId) {
    return new CommandHeaders(
      UUID.randomUUID().toString(),
      tenantId,
      null,
      null
    );
  }


}
