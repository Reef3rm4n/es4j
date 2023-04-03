package io.vertx.eventx.infrastructure.misc;


import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.binding.Binding;
import io.activej.inject.module.Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.eventx.objects.EventxModule;
import org.reflections.Reflections;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public class CustomClassLoader {

  protected static final Logger LOGGER = LoggerFactory.getLogger(CustomClassLoader.class);

  public static final String PACKAGE_NAME = System.getenv().getOrDefault("PACKAGE_NAME", "io.vertx.eventx");
  public static final Reflections REFLECTIONS = new Reflections("io.vertx.eventx");
  public static final Reflections REFLECTIONS_EXTERNAL = new Reflections(PACKAGE_NAME);

  public static <T> List<Class<? extends T>> getSubTypes(Class<T> tClass) {
    return REFLECTIONS_EXTERNAL.getSubTypesOf(tClass).stream().toList();
  }

  public static Collection<Module> loadModules() {
    return REFLECTIONS.getSubTypesOf(EventxModule.class).stream()
      .map(CustomClassLoader::instantiate)
      .map(foundCLass -> {
          LOGGER.info("Event.x module found -> " + foundCLass.getClass().getName());
          return (Module) foundCLass;
        }
      )
      .toList();
  }

  public static <T> Class<?> getFirstGenericType(T object) {
    Type[] genericInterfaces = object.getClass().getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + object.getClass().getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Validators should implement CommandValidator interface -> " + object.getClass().getName());
    }
    final var genericInterface = genericInterfaces[0];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
//      LOGGER.debug(object.getClass().getName() + " generic types -> " + Arrays.stream(genericTypes).map(Type::getTypeName).toList());
      try {
        return Class.forName(genericTypes[0].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

  public static Class<?> getFirstGenericType(Class<?> tclass) {
    Type[] genericInterfaces = tclass.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + tclass.getClass().getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Validators should implement CommandValidator interface -> " + tclass.getClass().getName());
    }
    final var genericInterface = genericInterfaces[0];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
//      LOGGER.debug(object.getClass().getName() + " generic types -> " + Arrays.stream(genericTypes).map(Type::getTypeName).toList());
      try {
        return Class.forName(genericTypes[0].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

  public static <T> Class<?> getSecondGenericType(T object) {
    Type[] genericInterfaces = object.getClass().getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + object.getClass().getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Validators should implement CommandValidator interface -> " + object.getClass().getName());
    }
    final var genericInterface = genericInterfaces[1];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
//      LOGGER.debug(object.getClass().getName() + " generic types -> " + Arrays.stream(genericTypes).map(Type::getTypeName).toList());
      try {
        return Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

  public static <I> Class<? extends I> getSecondGenericType(Class<?> tClass, Class<I> interfaceClass) {
    Type[] genericInterfaces = tClass.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + tClass.getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Validators should implement CommandValidator interface -> " + tClass.getName());
    }
    final var genericInterface = genericInterfaces[1];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
      try {
        return (Class<? extends I>) Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }

  private static <T> T instantiate(Class<T> tClass) {
    try {
      return tClass.getDeclaredConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      LOGGER.error("Unable to instantiate class -> " + tClass.getName(), e);
      throw new IllegalArgumentException(e);
    }
  }

  public static <T> List<T> loadFromInjector(final Injector injector, final Class<T> tClass) {
    return injector.getBindings().entrySet().stream()
      .filter(entry -> checkPresenceInBinding(tClass, entry))
      .map(entry -> injector.getInstance(Key.of(entry.getKey().getRawType(), entry.getKey().getQualifier())))
      .map(tClass::cast)
      .toList();
  }

  public static <T> boolean checkPresence(final Injector injector, final Class<T> tClass) {
    return injector.getBindings().entrySet().stream().anyMatch(entry -> checkPresenceInBinding(tClass, entry));
  }


  public static <T> boolean checkPresenceInBinding(Injector injector, Class<T> tClass) {
    LOGGER.debug("Looking for -> " + tClass.getName());
    final var matches = injector.getBindings().entrySet().stream()
      .filter(
        key -> checkPresenceInBinding(tClass, key)
      )
      .map(Map.Entry::getKey)
      .toList();
    LOGGER.debug("Found " + tClass + " bindings -> " + matches.stream().map(Key::getRawType).toList());
    return !matches.isEmpty();
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
