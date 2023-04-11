package io.vertx.eventx.sql.models;

import io.soabase.recordbuilder.core.RecordBuilder;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.vertx.eventx.sql.misc.Constants.*;



@RecordBuilder
public record BaseRecord(
    String tenantId,
    Integer version,
    Instant creationDate,
    Instant lastUpdate
) {

    public static BaseRecord newRecord(String tenantId) {
        return new BaseRecord(Objects.requireNonNullElse(tenantId, "default"), 0, Instant.now(), Instant.now());
    }

    public static BaseRecord newRecord() {
        return new BaseRecord("default", 0, Instant.now(), Instant.now());
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

    public BaseRecord withId(Long i) {
        return new BaseRecord(tenantId, version, creationDate, lastUpdate);
    }

    public BaseRecord withVersion(Integer c) {
        return new BaseRecord(tenantId, c, creationDate, lastUpdate);
    }

    public BaseRecord withLastUpdate(Instant i) {
        return new BaseRecord(tenantId, version, creationDate, i);
    }
}
