package io.vertx.eventx.sql.models;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.vertx.eventx.sql.misc.Constants.*;


public record RecordWithID(
    Long id,
    String tenantId,
    Integer version,
    Instant creationDate,
    Instant lastUpdate
) {

    public static RecordWithID newRecord(String tenantId) {
        return new RecordWithID(null, Objects.requireNonNullElse(tenantId, "default"), 0, Instant.now(), Instant.now());
    }

    public static RecordWithID newRecord() {
        return new RecordWithID(null, "default", 0, Instant.now(), Instant.now());
    }

    public Map<String, Object> params() {
        final var map = new HashMap<String, Object>();
        map.put(ID, id);
        map.put(VERSION, version);
        map.put(TENANT, tenantId);
        if (creationDate != null)
            map.put(CREATION_DATE, LocalDateTime.ofInstant(creationDate, ZoneOffset.UTC));
        if (lastUpdate != null)
            map.put(LAST_UPDATE, LocalDateTime.ofInstant(lastUpdate, ZoneOffset.UTC));
        return map;
    }

    public Map<String, Object> tenantLessParams() {
        final var map = new HashMap<String, Object>();
        map.put(ID, id);
        map.put(VERSION, version);
        if (creationDate != null)
            map.put(CREATION_DATE, LocalDateTime.ofInstant(creationDate, ZoneOffset.UTC));
        if (lastUpdate != null)
            map.put(LAST_UPDATE, LocalDateTime.ofInstant(lastUpdate, ZoneOffset.UTC));
        return map;
    }

    public RecordWithID withId(Long i) {
        return new RecordWithID(i, tenantId, version, creationDate, lastUpdate);
    }

    public RecordWithID withVersion(Integer c) {
        return new RecordWithID(id, tenantId, c, creationDate, lastUpdate);
    }

    public RecordWithID withLastUpdate(Instant i) {
        return new RecordWithID(id, tenantId, version, creationDate, i);
    }
}
