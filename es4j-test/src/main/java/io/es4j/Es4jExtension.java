package io.es4j;


import io.es4j.config.DatabaseConfiguration;
import io.es4j.config.DatabaseConfigurationCache;
import io.es4j.config.orm.ConfigurationKey;
import io.es4j.core.objects.AggregateState;
import io.es4j.infrastructure.cache.CaffeineWrapper;
import io.es4j.infrastructure.config.FileConfigurationCache;
import io.es4j.infrastructure.models.AggregatePlainKey;
import io.es4j.infrastructure.proxy.AggregateEventBusPoxy;
import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;

import static io.es4j.config.DatabaseConfigurationService.parseKey;


public class Es4jExtension implements BeforeAllCallback, AfterAllCallback, Extension, ParameterResolver, BeforeEachCallback, AfterEachCallback {
  private Bootstrapper<? extends Aggregate> bootstrapper;
  private final Logger LOGGER = LoggerFactory.getLogger(Es4jExtension.class);


  @Override
  public void beforeAll(ExtensionContext extensionContext) {
    extensionContext.getTestClass().ifPresent(
      testClass -> {
        Es4jTest annotation = testClass.getAnnotation(Es4jTest.class);
        bootstrapper = new Bootstrapper<>(annotation.aggregate())
          .setPostgres(annotation.infrastructure())
          .setRemoteHost(annotation.host())
          .setRemotePort(annotation.port());
        bootstrapper.bootstrap();
      }
    );
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) {
    if (bootstrapper != null) {
      bootstrapper.destroy();
    }
  }


  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == AggregateEventBusPoxy.class
      || parameterContext.getParameter().getType() == AggregateHttpClient.class;
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    if (bootstrapper != null) {
      if (parameterContext.getParameter().getType().isAssignableFrom(AggregateEventBusPoxy.class)) {
        return bootstrapper.eventBusPoxy;
      } else if (parameterContext.getParameter().getType().isAssignableFrom(AggregateHttpClient.class)) {
        return bootstrapper.httpClient;
      }
    }
    throw new IllegalStateException("Bootstrapper has not been initialized");
  }


  private List<Tuple4<Class<? extends DatabaseConfiguration>, String, String, Integer>> databaseConfigurations(Method testMethod) {
    LOGGER.debug("Getting business rules from method {}", testMethod);
    List<Tuple4<Class<? extends DatabaseConfiguration>, String, String, Integer>> configurationTuples = new ArrayList<>();
    io.es4j.DatabaseBusinessRule[] annotations = testMethod.getAnnotationsByType(io.es4j.DatabaseBusinessRule.class);
    if (annotations != null && !Arrays.stream(annotations).toList().isEmpty()) {
      Arrays.stream(annotations).forEach(a -> configurationTuples.add(Tuple4.of(a.configurationClass(), a.fileName(), a.tenant(), a.version())));
      return configurationTuples;
    }
    return new ArrayList<>();
  }

  private List<String> fileConfigurations(Method testMethod) {
    LOGGER.debug("Getting business rules from method {}", testMethod);
    List<String> configurationTuples = new ArrayList<>();
    FileBusinessRule[] annotations = testMethod.getAnnotationsByType(FileBusinessRule.class);
    if (annotations != null && !Arrays.stream(annotations).toList().isEmpty()) {
      Arrays.stream(annotations).forEach(a -> configurationTuples.add(a.fileName()));
      return configurationTuples;
    }
    return new ArrayList<>();
  }

  @Override
  public void afterEach(ExtensionContext context) {
    context.getTestMethod().ifPresent(
      testMethod -> {
        final var databaseConfigurations = databaseConfigurations(testMethod);
        if (!databaseConfigurations.isEmpty()) {
          LOGGER.info("Removing previously deployed database configurations {}", databaseConfigurations);
          databaseConfigurations.forEach(
            dbConfig -> {
              LOGGER.info("Deleting database configuration {}", dbConfig);
              DatabaseConfigurationCache.invalidate(parseKey(new ConfigurationKey(dbConfig.getItem1().getName(), dbConfig.getItem4(), dbConfig.getItem3())));
            }
          );
        }
        final var fileConfigurations = fileConfigurations(testMethod);
        if (!fileConfigurations.isEmpty()) {
          LOGGER.info("Removing previously deployed configurations {}", databaseConfigurations);
          fileConfigurations.forEach(
            filename -> {
              LOGGER.info("Deleting file configuration {}", filename);
              FileConfigurationCache.invalidate(filename.substring(0, filename.indexOf(".")));
            }
          );
        }
        final var optionalGivenAggregate = Optional.ofNullable(testMethod.getAnnotation(GivenAggregate.class));
        optionalGivenAggregate.ifPresent(this::dropAggregate);
      }
    );
  }

  private void dropAggregate(GivenAggregate givenAggregate) {
    final var jsonObject = Bootstrapper.vertx.fileSystem().readFileBlocking(givenAggregate.filename()).toJsonObject();
    final var state = getState(bootstrapper.aggregateClass, jsonObject);
    CaffeineWrapper.invalidate(bootstrapper.aggregateClass, new AggregatePlainKey(
      bootstrapper.aggregateClass.getName(),
      state.state().aggregateId(),
      state.state().tenant()
    ));
    // todo drop events ?
  }

  private void addAggregate(GivenAggregate givenAggregate) {
    final var jsonObject = Bootstrapper.vertx.fileSystem().readFileBlocking(givenAggregate.filename()).toJsonObject();
    final var state = getState(bootstrapper.aggregateClass, jsonObject);
    CaffeineWrapper.put(
      new AggregatePlainKey(
        bootstrapper.aggregateClass.getName(),
        state.state().aggregateId(),
        state.state().tenant()
      ),
      state
    );
  }


  public <T extends Aggregate> AggregateState<T> getState(Class<T> aggregateClass, JsonObject jsonObject) {
    final var aggregateState = new AggregateState<>(aggregateClass);
    return aggregateState.setState(jsonObject.mapTo(aggregateClass));
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    context.getTestMethod().ifPresent(
      testMethod -> {
        final var fileNames = fileConfigurations(testMethod);
        if (!fileNames.isEmpty()) {
          fileNames.forEach(
            filename -> {
              final var configuration = Bootstrapper.vertx.fileSystem().readFileBlocking(filename)
                .toJsonObject();
              LOGGER.info("Adding file configuration {} {}", filename, configuration.encodePrettily());
              FileConfigurationCache.put(filename.substring(0, filename.indexOf(".")), configuration);
            }
          );
        }
        final var databaseConfigurations = databaseConfigurations(testMethod);
        if (!databaseConfigurations.isEmpty()) {
          databaseConfigurations.forEach(
            fsConfig -> {
              final var configuration = Bootstrapper.vertx.fileSystem().readFileBlocking(fsConfig.getItem2())
                .toJsonObject();
              LOGGER.info("Adding database configuration {} {}", fsConfig, configuration.encodePrettily());
              DatabaseConfigurationCache.put(parseKey(new ConfigurationKey(fsConfig.getItem1().getName(), 0, fsConfig.getItem3())), configuration);
            }
          );
          final var optionalGivenAggregate = Optional.ofNullable(testMethod.getAnnotation(GivenAggregate.class));
          optionalGivenAggregate.ifPresent(this::addAggregate);
        }
      }
    );
  }
}
