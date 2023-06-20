package io.eventx.infrastructure.misc;


import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.binding.Binding;
import io.activej.inject.module.Module;
import io.eventx.Aggregate;
import io.eventx.Bootstrap;
import io.eventx.core.objects.EventxModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;


public class Loader {


  protected static final Logger LOGGER = LoggerFactory.getLogger(Loader.class);

  public static Collection<Module> eventxModules() {
    return ServiceLoader.load(EventxModule.class).stream().map(
        ServiceLoader.Provider::get
      )
      .map(eventxModule -> (Module) eventxModule)
      .toList();
  }

  public static List<Class<? extends Aggregate>> loadAggregates() {
    final List<Class<? extends Aggregate>> classes = new ArrayList<>();
    ServiceLoader.load(Bootstrap.class).stream()
      .map(ServiceLoader.Provider::get)
      .forEach(eventxModule -> classes.add(eventxModule.aggregateClass()));
    return classes;
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


  public static <T> List<T> loadFromInjector(final Injector injector, final Class<T> tClass) {
    return injector.getBindings().entrySet().stream()
      .filter(entry -> checkPresenceInBinding(tClass, entry))
      .map(entry -> injector.getInstance(Key.of(entry.getKey().getRawType(), entry.getKey().getQualifier())))
      .map(tClass::cast)
      .toList();
  }

  public static <T> List<T> loadFromInjectorClass(final Injector injector, final Class<T> tClass) {
    return injector.getBindings().entrySet().stream()
      .filter(entry -> checkPresenceInBinding(tClass, entry))
      .map(entry -> injector.getInstance(entry.getKey()))
      .map(tClass::cast)
      .toList();
  }

  public static <T> boolean checkPresence(final Injector injector, final Class<T> tClass) {
    return injector.getBindings().entrySet().stream().anyMatch(entry -> checkPresenceInBinding(tClass, entry));
  }

  public static <T> boolean checkPresenceInBinding(final Class<T> tClass, final Map.Entry<Key<?>, Binding<?>> entry) {
    return Arrays.stream(entry.getKey().getRawType().getInterfaces()).anyMatch(i -> i.isAssignableFrom(tClass))
      || entry.getKey().getRawType().isAssignableFrom(tClass);
  }

  public static <T> boolean checkPresenceInModules(final Class<T> tClass, final Collection<Module> modules) {
    return modules.stream().anyMatch(m -> checkPresenceInModule(tClass, m));
  }

  public static <T> boolean checkPresenceInModule(final Class<T> tClass, final Module module) {
    return module.getBindings().get().entrySet().stream().anyMatch(b ->
      b.getValue().stream().anyMatch(bb -> checkPresenceInBinding(tClass, Map.entry(b.getKey(), bb)))
    );
  }

}
