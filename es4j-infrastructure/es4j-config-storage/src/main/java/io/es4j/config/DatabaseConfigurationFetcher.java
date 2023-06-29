package io.es4j.config;


import java.util.*;

public class DatabaseConfigurationFetcher {

  private DatabaseConfigurationFetcher() {
  }

  public static <T extends DatabaseConfiguration> Optional<T> fetch(Class<T> tClass, String tenant) {
    final var data = DatabaseConfigurationCache.get(parseKey(tenant, tClass));
    if (Objects.nonNull(data)) {
      return Optional.of(data.mapTo(tClass));
    }
    return Optional.empty();
  }

  public static <T extends DatabaseConfiguration> Optional<T> fetch(Class<T> tClass) {
    final var data = DatabaseConfigurationCache.get(parseKey("default", tClass));
    if (Objects.nonNull(data)) {
      return Optional.of(data.mapTo(tClass));
    }
    return Optional.empty();
  }

  private static <T extends DatabaseConfiguration> String parseKey(String tenant, Class<T> tClass) {
    return new StringJoiner("::")
      .add(tenant)
      .add(tClass.getName())
      .toString();
  }

}
