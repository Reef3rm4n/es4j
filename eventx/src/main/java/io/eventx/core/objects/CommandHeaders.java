package io.eventx.core.objects;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.soabase.recordbuilder.core.RecordBuilder;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
@RecordBuilder
public record CommandHeaders(
  String commandId,
  String token
) implements Serializable {

  public static CommandHeaders simple(String tenantID, String commandID) {
    return new CommandHeaders(
      commandID,
      null
    );
  }

  public static CommandHeaders simple(String commandID) {
    return new CommandHeaders(
      commandID,
      null
    );
  }

  public static CommandHeaders defaultHeaders() {
    return new CommandHeaders(
      UUID.randomUUID().toString(),
      null
    );
  }


}
