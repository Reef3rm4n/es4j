package io.vertx.skeleton.evs.cache;

public class AddressResolver {

  private AddressResolver() {
  }

  public static <T> String localAvailableHandlerCounter(Class<T> tClass, String handlerAddress) {
    return tClass.getName() + "." + handlerAddress + "." + ".local.handler.counter";
  }
  public static <T> String localAggregateHandler(Class<T> tClass) {
    return tClass.getName() + ".local.entity.aggregate.handler";
  }

  public static <T> String localAvailableHandlers(Class<T> tClass) {
    return tClass.getName() + ".local.available.handlers";
  }

  public static <T> String synchronizerService(Class<T> tClass) {
    return tClass.getName() + ".synchronizer";
  }

  public static <T> String eventConsumer(final Class<T> tClass) {
    return tClass.getClass() + ".event.consumer";
  }
}
