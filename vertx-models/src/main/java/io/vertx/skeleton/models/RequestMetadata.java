package io.vertx.skeleton.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.ws.rs.HeaderParam;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RequestMetadata(
  @HeaderParam(CLIENT_ID) String clientId,
  @HeaderParam(CHANNEL_ID) String channelId,
  @HeaderParam(EXT_SYSTEM_ID) String externalSystemId,
  @HeaderParam(X_TXT_ID) String txId,
  @HeaderParam(TXT_DATE) String txtDate,
  @HeaderParam(BRAND_ID_HEADER) Integer brandId,
  @HeaderParam(PARTNER_ID_HEADER) Integer partnerId,
  @HeaderParam(PLAYER_ID) String userId,
  @HeaderParam(LONG_TERM_TOKEN) String longTermToken
) implements Serializable {
  public static final String CLIENT_ID = "X-CLIENT-ID";
  public static final String CHANNEL_ID = "X-CHANNEL-ID";
  public static final String EXT_SYSTEM_ID = "X-EXT-SYSTEM-ID";
  public static final String X_TXT_ID = "X-TXT-ID";
  public static final String TXT_DATE = "X-TXT-DATE";
  public static final String BRAND_ID_HEADER = "X-BRAND-ID";
  public static final String PARTNER_ID_HEADER = "X-PARTNER-ID";
  public static final String PLAYER_ID = "X-PLAYER-ID";
  public static final String LONG_TERM_TOKEN = "X-CONNECT-SESSION-ID";

  public Tenant tenant() {
    return new Tenant(brandId, partnerId);
  }


  public static RequestMetadata from(Tenant tenant, String txId) {
    return new RequestMetadata(
      null,
      null,
      null,
      txId,
      null,
      tenant.brandId(),
      tenant.partnerId(),
      null,
      null
    );
  }


}
