package io.vertx.skeleton.sql.models;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.vertx.skeleton.sql.misc.Constants.*;


public record RecordWithoutID(
    String tenantId,
    Integer version,
    Instant creationDate,
    Instant lastUpdate
) {

    public static RecordWithoutID newRecord(String tenantId) {
        return new RecordWithoutID(Objects.requireNonNullElse(tenantId, "default"), 0, Instant.now(), Instant.now());
    }

    public static RecordWithoutID newRecord() {
        return new RecordWithoutID("default", 0, Instant.now(), Instant.now());
    }

    public Map<String, Object> params() {
        final var map = new HashMap<String, Object>();
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
        map.put(VERSION, version);
        if (creationDate != null)
            map.put(CREATION_DATE, LocalDateTime.ofInstant(creationDate, ZoneOffset.UTC));
        if (lastUpdate != null)
            map.put(LAST_UPDATE, LocalDateTime.ofInstant(lastUpdate, ZoneOffset.UTC));
        return map;
    }

    public RecordWithoutID withId(Long i) {
        return new RecordWithoutID(tenantId, version, creationDate, lastUpdate);
    }

    public RecordWithoutID withVersion(Integer c) {
        return new RecordWithoutID(tenantId, c, creationDate, lastUpdate);
    }

    public RecordWithoutID withLastUpdate(Instant i) {
        return new RecordWithoutID(tenantId, version, creationDate, i);
    }
}
