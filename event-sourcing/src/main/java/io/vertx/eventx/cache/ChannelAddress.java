package io.vertx.eventx.cache;

import io.vertx.eventx.Aggregate;

public class ChannelAddress {

  private ChannelAddress() {
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
    return "/" + aggregateClass.getSimpleName().toLowerCase() + "/bridge/command";
  }
}
