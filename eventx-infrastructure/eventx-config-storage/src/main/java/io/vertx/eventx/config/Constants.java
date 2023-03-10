package io.vertx.eventx.config;

public class Constants {
  public static final Boolean KUBERNETES = Boolean.parseBoolean(System.getenv().getOrDefault("KUBERNETES", "false"));
  public static final String KUBERNETES_NAMESPACE = System.getenv().getOrDefault("KUBERNETES_NAMESPACE", "default");
}
