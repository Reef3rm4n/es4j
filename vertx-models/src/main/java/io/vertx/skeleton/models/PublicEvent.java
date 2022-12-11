package io.vertx.skeleton.models;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import io.vertx.core.json.JsonObject;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class PublicEvent {

  private String txId;
  private String uniqueId;
  private String type;
  private JsonObject event;

  public PublicEvent(final String txId, final String uniqueId, final String type, final JsonObject event) {
    this.txId = txId;
    this.uniqueId = uniqueId;
    this.type = type;
    this.event = event;
  }

  public String txId() {
    return txId;
  }

  public String uniqueId() {
    return uniqueId;
  }

  public String type() {
    return type;
  }

  public JsonObject event() {
    return event;
  }

  public PublicEvent(final JsonObject jsonObject) {
    final var json = jsonObject.mapTo(PublicEvent.class);
    this.txId = json.txId();
    this.uniqueId = json.uniqueId();
    this.type = json.type();
    this.event = json.event();
  }
  public JsonObject toJson() {
    return JsonObject.mapFrom(this);
  }
}
