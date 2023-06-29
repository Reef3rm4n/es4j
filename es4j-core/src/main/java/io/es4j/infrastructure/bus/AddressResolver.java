package io.es4j.infrastructure.bus;

import io.es4j.Aggregate;
import io.es4j.Command;

import static io.es4j.core.CommandHandler.camelToKebab;

public class AddressResolver {

  private AddressResolver() {
  }


  public static <T extends Aggregate> String broadcastChannel(Class<T> tClass) {
    return "/" + camelToKebab(tClass.getSimpleName()) + "/available/node";
  }

  public static <T extends Aggregate> String invokeChannel(Class<T> tClass) {
    return "/" + camelToKebab(tClass.getSimpleName()) + "/available/nodes";
  }

  public static <T extends Aggregate> String nodeAddress(Class<T> aggregateClass, String deploymentID) {
    return "/" + camelToKebab(aggregateClass.getSimpleName()) + "/" + deploymentID;
  }

  public static <T extends Aggregate> String commandConsumer(Class<T> aggregateClass, String deploymentID, Class<? extends Command> commandClass) {
    return "/" + camelToKebab(aggregateClass.getSimpleName()) + "/" + deploymentID + "/" + camelToKebab(commandClass.getSimpleName());
  }

  public static <T extends Aggregate> String commandBridge(Class<T> aggregateClass, Class<? extends Command> commandClass) {
    return "/" + camelToKebab(aggregateClass.getSimpleName()) + "/" + camelToKebab(commandClass.getSimpleName());
  }


  public static String resolveCommandConsumer(String nodeAddress, Class<? extends Command> commandClass) {
    return nodeAddress + "/" + camelToKebab(commandClass.getSimpleName());
  }
}
