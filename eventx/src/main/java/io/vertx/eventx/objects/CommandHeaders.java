package io.vertx.eventx.objects;



import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.io.Serializable;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
@RecordBuilder
public record CommandHeaders(
  String commandId,
  String tenantId,
  String token
) implements Serializable {

  public static CommandHeaders simple(String tenantID, String commandID) {
    return new CommandHeaders(
      commandID,
      tenantID,
      null
    );
  }

  public static CommandHeaders simple(String commandID) {
    return new CommandHeaders(
      commandID,
      "default",
      null
    );
  }

  public static CommandHeaders defaultHeaders() {
    return new CommandHeaders(
      UUID.randomUUID().toString(),
      "default",
      null
    );
  }
  public static CommandHeaders defaultHeaders(String tenantId) {
    return new CommandHeaders(
      UUID.randomUUID().toString(),
      tenantId,
      null
    );
  }


}
