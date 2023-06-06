package io.eventx;


import io.eventx.config.BusinessRule;
import io.eventx.config.DbConfigCache;
import io.eventx.config.FsConfigCache;
import io.eventx.config.orm.ConfigurationKey;
import io.eventx.infrastructure.proxy.AggregateEventBusPoxy;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple4;
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.eventx.launcher.ConfigLauncher.parseKey;

public class EventxExtension implements BeforeAllCallback, AfterAllCallback, Extension, ParameterResolver, BeforeEachCallback, AfterEachCallback {
  private Bootstrapper<? extends Aggregate> bootstrapper;

  private final Logger LOGGER = LoggerFactory.getLogger(EventxExtension.class);


  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    extensionContext.getTestClass().ifPresent(
      testClass -> {
        EventxTest annotation = testClass.getAnnotation(EventxTest.class);
        bootstrapper = new Bootstrapper<>(annotation.aggregate())
          .setPostgres(annotation.infrastructure())
          .setRemoteHost(annotation.host())
          .setRemotePort(annotation.port());
        bootstrapper.bootstrap();
      }
    );
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    if (bootstrapper != null) {
      bootstrapper.destroy();
    }
  }


  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == AggregateEventBusPoxy.class ||
      parameterContext.getParameter().getType() == AggregateHttpClient.class;
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    if (bootstrapper != null) {
      if (parameterContext.getParameter().getType().isAssignableFrom(AggregateEventBusPoxy.class)) {
        return bootstrapper.eventBusPoxy;
      } else if (parameterContext.getParameter().getType().isAssignableFrom(AggregateHttpClient.class)) {
        return bootstrapper.httpClient;
      } else {
        throw new IllegalArgumentException("Unsupported class");
      }
    }
    throw new IllegalStateException("Bootstrapper has not been initialized");
  }


  private List<Tuple4<Class<? extends BusinessRule>, String, String, Integer>> databaseConfigurations(Method testMethod) {
    LOGGER.debug("Getting business rules from method {}", testMethod);
    List<Tuple4<Class<? extends io.eventx.config.BusinessRule>, String, String, Integer>> configurationTuples = new ArrayList<>();
    DatabaseBusinessRule[] annotations = testMethod.getAnnotationsByType(DatabaseBusinessRule.class);
    if (annotations != null && !Arrays.stream(annotations).toList().isEmpty()) {
      Arrays.stream(annotations).forEach(a -> configurationTuples.add(Tuple4.of(a.value(), a.params(), a.tenant(), a.version())));
      return configurationTuples;
    }
    return new ArrayList<>();
  }

  private List<Tuple2<Class<? extends io.eventx.config.BusinessRule>, String>> fileConfigurations(Method testMethod) {
    LOGGER.debug("Getting business rules from method {}", testMethod);
    List<Tuple2<Class<? extends io.eventx.config.BusinessRule>, String>> configurationTuples = new ArrayList<>();
    DatabaseBusinessRule[] annotations = testMethod.getAnnotationsByType(DatabaseBusinessRule.class);
    if (annotations != null && !Arrays.stream(annotations).toList().isEmpty()) {
      Arrays.stream(annotations).forEach(a -> configurationTuples.add(Tuple2.of(a.value(), a.params())));
      return configurationTuples;
    }
    return new ArrayList<>();
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    context.getTestMethod().ifPresent(
      testMethod -> {
        final var databaseConfigurations = databaseConfigurations(testMethod);
        if (!databaseConfigurations.isEmpty()) {
          LOGGER.info("Removing previously deployed database configurations {}", databaseConfigurations);
          databaseConfigurations.forEach(
            dbConfig -> {
              LOGGER.info("Deleting database configuration {}", dbConfig);
              DbConfigCache.delete(parseKey(new ConfigurationKey(dbConfig.getItem1().getName(), dbConfig.getItem4(), dbConfig.getItem3())));
            }
          );
        }
        final var fileConfigurations = fileConfigurations(testMethod);
        if (!fileConfigurations.isEmpty()) {
          LOGGER.info("Removing previously deployed configurations {}", databaseConfigurations);
          fileConfigurations.forEach(
            fsConfig -> {
              LOGGER.info("Deleting file configuration {}", fsConfig);
              FsConfigCache.delete(fsConfig.getItem2());
            }
          );
        }
      }
    );
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    context.getTestMethod().ifPresent(
      testMethod -> {
        final var configs = fileConfigurations(testMethod);
        if (!configs.isEmpty()) {
          configs.forEach(
            config -> {
              LOGGER.info("Adding file configuration {}", config);
              final var configuration = Bootstrapper.vertx.fileSystem().readFileBlocking(config.getItem2())
                .toJsonObject();
              FsConfigCache.put(config.getItem2(), configuration);
            }
          );
        }
        final var databaseConfigurations = databaseConfigurations(testMethod);
        if (!databaseConfigurations.isEmpty()) {

          databaseConfigurations.forEach(
            fsConfig -> {
              LOGGER.info("Adding database configuration {}", fsConfig);
              final var configuration = Bootstrapper.vertx.fileSystem().readFileBlocking(fsConfig.getItem2())
                .toJsonObject();
              DbConfigCache.put(parseKey(new ConfigurationKey(fsConfig.getItem1().getName(), 0, fsConfig.getItem3())), configuration);
            }
          );
        }
      }
    );
  }
}
