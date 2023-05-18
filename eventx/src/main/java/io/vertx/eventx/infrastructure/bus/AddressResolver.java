package io.vertx.eventx.infrastructure.bus;

import io.vertx.eventx.Aggregate;

import static io.vertx.eventx.core.AggregateVerticleLogic.camelToKebab;

public class AddressResolver {

  private AddressResolver() {
  }


  public static <T extends Aggregate> String broadcastChannel(Class<T> tClass) {
    return "/" + tClass.getSimpleName().toLowerCase() + "/available/actor";
  }

  public static <T extends Aggregate> String invokeChannel(Class<T> tClass) {
    return "/" + tClass.getSimpleName().toLowerCase() + "/available/actors";
  }

  public static <T extends Aggregate> String commandConsumer(Class<T> entityClass, String deploymentID) {
    return "/" + entityClass.getSimpleName().toLowerCase() + "/" + deploymentID;
  }

  public static <T extends Aggregate> String commandBridge(Class<T> aggregateClass) {
    return "command-bridge/" + camelToKebab(aggregateClass.getSimpleName());
  }
}
