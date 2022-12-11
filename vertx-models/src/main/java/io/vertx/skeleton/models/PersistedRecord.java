

/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.skeleton.models;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;


public record PersistedRecord(
  Long id,
  Tenant tenant,
  Integer version,
  Instant creationDate,
  Instant lastUpdate
) {

  public static PersistedRecord newRecord(Tenant tenant) {
    return new PersistedRecord(null, tenant, 0, Instant.now(), Instant.now());
  }

  public static PersistedRecord tenantLess() {
    return new PersistedRecord(null, null, 0, Instant.now(), Instant.now());
  }

  public Map<String, Object> params() {
    final var map = new HashMap<String, Object>();
    map.put("id", id);
    map.put("version", version);
    map.put("tenant", tenant.generateString());
    if (creationDate != null)
      map.put("creation_date", LocalDateTime.ofInstant(creationDate, ZoneOffset.UTC));
    if (lastUpdate != null)
      map.put("last_update", LocalDateTime.ofInstant(lastUpdate, ZoneOffset.UTC));
    return map;
  }

  public Map<String, Object> tenantLessParams() {
    final var map = new HashMap<String, Object>();
    map.put("id", id);
    map.put("version", version);
    if (creationDate != null)
      map.put("creation_date", LocalDateTime.ofInstant(creationDate, ZoneOffset.UTC));
    if (lastUpdate != null)
      map.put("last_update", LocalDateTime.ofInstant(lastUpdate, ZoneOffset.UTC));
    return map;
  }

  public PersistedRecord withId(Long i) {
    return new PersistedRecord(i, tenant, version, creationDate, lastUpdate);
  }

  public PersistedRecord withVersion(Integer c) {
    return new PersistedRecord(id, tenant, c, creationDate, lastUpdate);
  }

  public PersistedRecord withLastUpdate(Instant i) {
    return new PersistedRecord(id, tenant, version, creationDate, i);
  }
}
