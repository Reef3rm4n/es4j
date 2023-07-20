package io.es4j.infrastructure.misc;


import io.es4j.*;
import io.es4j.infrastructure.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;


public class Es4jServiceLoader {


  protected static final Logger LOGGER = LoggerFactory.getLogger(Es4jServiceLoader.class);

  public static List<Behaviour> loadBehaviours() {
    return ServiceLoader.load(Behaviour.class).stream()
      .map(ServiceLoader.Provider::get)
      .toList();
  }

  public static Optional<AggregateCache> loadCache() {
    return ServiceLoader.load(AggregateCache.class).stream()
      .findFirst()
      .map(ServiceLoader.Provider::get);
  }

  public static EventStore loadEventStore() {
    return ServiceLoader.load(EventStore.class).stream()
      .findFirst()
      .map(ServiceLoader.Provider::get)
      .orElseThrow(() -> new IllegalStateException("EventStore not found"));
  }

  public static List<AsyncStateTransfer> stateProjections() {
    return ServiceLoader.load(AsyncStateTransfer.class).stream()
      .map(ServiceLoader.Provider::get)
      .toList();
  }

  public static List<AsyncProjection> pollingEventProjections() {
    return ServiceLoader.load(AsyncProjection.class).stream()
      .map(ServiceLoader.Provider::get)
      .toList();
  }

  public static List<InlineProjection> liveEventProjections() {
    return ServiceLoader.load(InlineProjection.class).stream()
      .map(ServiceLoader.Provider::get)
      .toList();
  }

  public static List<InlineStateTransfer> liveStateProjections() {
    return ServiceLoader.load(InlineStateTransfer.class).stream()
      .map(ServiceLoader.Provider::get)
      .toList();
  }

  public static OffsetStore loadOffsetStore() {
    return ServiceLoader.load(OffsetStore.class).stream()
      .findFirst()
      .map(ServiceLoader.Provider::get)
      .orElseThrow(() -> new IllegalStateException("OffsetStore not found"));
  }

  public static List<Aggregator> loadAggregators() {
    return ServiceLoader.load(Aggregator.class).stream()
      .map(ServiceLoader.Provider::get)
      .toList();
  }

  public static List<Es4jDeployment> bootstrapList() {
    return ServiceLoader.load(Es4jDeployment.class).stream()
      .map(ServiceLoader.Provider::get)
      .peek(aggregate -> {
        LOGGER.info("Bootstrapper found {}", aggregate);
      })
      .toList();
  }

  public static <T> Class<?> getFirstGenericType(T object) {
    return getFirstGenericType(object.getClass());
  }

  public static Class<?> getFirstGenericType(Class<?> tclass) {
    Type[] genericInterfaces = tclass.getGenericInterfaces();
    final var genericInterface = genericInterfaces[0];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
      try {
        return Class.forName(genericTypes[0].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get generic types -> ", e);
      }
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

  public static <T> Class<?> getSecondGenericType(T object) {
    return getSecondGenericType(object.getClass());
  }

  public static <T> Class<?> getSecondGenericType(Class<T> tClass) {
    Type[] genericInterfaces = tClass.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviour " + tClass.getName() + " implements more than one interface");
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Behaviour " + tClass.getName() + " should implement one interface");
    }
    final var genericInterface = genericInterfaces[1];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
      try {
        return Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get generic type", e);
      }
    } else {
      throw new IllegalArgumentException("Invalid generic interface" + genericInterface.getClass());
    }
  }


  public static List<AggregateServices> loadAggregateServices() {
    return ServiceLoader.load(AggregateServices.class).stream()
      .map(ServiceLoader.Provider::get)
      .toList();
  }

  public static List<Bridge> loadBridges() {
    return ServiceLoader.load(Bridge.class)
      .stream().map(ServiceLoader.Provider::get)
      .toList();
  }
}
