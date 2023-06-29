package io.es4j.core.objects;

public enum ErrorSource {
  OFFSET_STORE,
  EVENT_STORE,
  INTERNAL_BUS,
  CACHE,
  CONFIG_STORAGE,
  INFRASTRUCTURE,
  LOGIC,
  UNKNOWN
}
