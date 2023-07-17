package io.es4j.infrastructure.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.smallrye.mutiny.Uni;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.config.ConfigRetriever;
import io.vertx.mutiny.core.Promise;
import io.vertx.mutiny.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;


public class Es4jConfigurationHandler {
  public static final List<ConfigRetriever> CONFIG_RETRIEVERS = new ArrayList<>();
  public static final Boolean KUBERNETES = Boolean.parseBoolean(System.getenv().getOrDefault("KUBERNETES", "false"));
  public static final String KUBERNETES_NAMESPACE = System.getenv().getOrDefault("KUBERNETES_NAMESPACE", "default");
  private static final Logger LOGGER = LoggerFactory.getLogger(Es4jConfigurationHandler.class);
  public static final String CONFIGURATION_FORMAT = System.getenv().getOrDefault("CONFIGURATION_FORMAT", "json");

  private Es4jConfigurationHandler() {
  }

  public static void configure(Vertx vertx, String configurationName, Consumer<JsonObject> configurationConsumer) {
    final var configRetrieverOptions = new ConfigRetrieverOptions();
    if (Boolean.TRUE.equals(KUBERNETES)) {
      LOGGER.info("Kubernetes config store activated for {}", configurationName);
      ConfigStoreOptions kubernetesStore = new ConfigStoreOptions()
        .setType("configmap")
        .setFormat(CONFIGURATION_FORMAT)
        .setOptional(false)
        .setConfig(new JsonObject()
          .put("namespace", KUBERNETES_NAMESPACE)
          .put("fileName", configurationName)
        );
      configRetrieverOptions.addStore(kubernetesStore);
      final var configurationRetriever = ConfigRetriever.create(vertx, configRetrieverOptions);
      configurationRetriever.listen(
        configChange -> {
          LOGGER.debug("new config {} \n  previous config {}", configChange.getNewConfiguration().encodePrettily(), configChange.getPreviousConfiguration().encodePrettily());
          JsonObject config;
          if (CONFIGURATION_FORMAT.equals("yaml")) {
            var stringConfig = configChange.getNewConfiguration().getString(
              configChange.getNewConfiguration()
                .getMap().keySet().stream().findAny()
                .orElseThrow()
            );
            config = parseConfiguration(stringConfig);
          } else {
            config = configChange.getNewConfiguration().getJsonObject(
              configChange.getNewConfiguration()
                .getMap().keySet().stream().findAny()
                .orElseThrow()
            );
          }
          configurationConsumer.accept(config);
        }
      );
    } else {
      LOGGER.info("{} configuration store activated ", configurationName + "." + CONFIGURATION_FORMAT);
      ConfigStoreOptions fileStore = new ConfigStoreOptions()
        .setType("file")
        .setFormat(CONFIGURATION_FORMAT)
        .setOptional(false)
        .setConfig(new JsonObject().put("path", configurationName + "." + CONFIGURATION_FORMAT));
      configRetrieverOptions.addStore(fileStore);
      final var retriever = ConfigRetriever.create(vertx, configRetrieverOptions);
      retriever.getConfig()
        .invoke(configurationConsumer)
        .subscribe()
        .with(
          avoid -> LOGGER.info("{} consumed ", configurationName),
          throwable -> LOGGER.error("{} consumption failed", configurationName, throwable)
        );
      CONFIG_RETRIEVERS.add(retriever);
    }
  }
  public static Uni<Void> startConfiguration(Vertx vertx, String file, Consumer<JsonObject> configurationConsumer) {
    if (Objects.nonNull(file)) {
      final var promise = Promise.promise();
      configure(
        vertx,
        file,
        newConfiguration -> {
          try {
           configurationConsumer.accept(newConfiguration);
            if (Objects.nonNull(promise)) {
              promise.complete();
            }
          } catch (Exception e) {
            LOGGER.error("Unable to consume file configuration {} {}", file, newConfiguration, e);
            if (Objects.nonNull(promise)) {
              promise.fail(e);
            }
          }
        }
      );
      return promise.future().replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }
  public static Uni<Void> fsConfigurations(Vertx vertx, List<String> files) {
    if (!files.isEmpty()) {
      final var promiseMap = files.stream().map(cfg -> Map.entry(cfg, Promise.promise()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      files.forEach(
        fileConfiguration -> configure(
          vertx,
          fileConfiguration,
          newConfiguration -> {
            try {
              LOGGER.info("Caching file configuration {} {}", fileConfiguration, newConfiguration);
              FileConfigurationCache.put(fileConfiguration, newConfiguration);
              final var promise = promiseMap.get(fileConfiguration);
              if (promise != null) {
                promise.complete();
              }
            } catch (Exception e) {
              LOGGER.error("Unable to consume file configuration {} {}", fileConfiguration, newConfiguration, e);
              final var promise = promiseMap.get(fileConfiguration);
              if (promise != null) {
                promise.complete();
              }
            }
          }
        )
      );
      return Uni.join().all(promiseMap.values().stream().map(Promise::future).toList())
        .andFailFast().invoke(avoid -> promiseMap.clear())
        .replaceWithVoid();
    }
    return Uni.createFrom().voidItem();
  }


  private static JsonObject parseConfiguration(final String config) {
    try {
      final var mapper = new ObjectMapper(new YAMLFactory());
      final var finalConfiguration = JsonObject.mapFrom(mapper.readValue(config, Object.class));
      LOGGER.info("{} parsed {} ", config, finalConfiguration.encodePrettily());
      return finalConfiguration;
    } catch (JsonProcessingException jsonProcessingException) {
      LOGGER.error("Failed to parse {}", config, jsonProcessingException);
      throw new IllegalArgumentException(jsonProcessingException);
    }
  }

  public static void close() {
    CONFIG_RETRIEVERS.forEach(ConfigRetriever::close);
  }

}
