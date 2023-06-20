package io.eventx.infrastructure.bus;

import io.eventx.Aggregate;
import io.eventx.Command;

import static io.eventx.core.CommandHandler.camelToKebab;

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
