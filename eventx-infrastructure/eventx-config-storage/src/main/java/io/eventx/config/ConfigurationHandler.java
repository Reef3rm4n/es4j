package io.eventx.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.config.ConfigRetriever;
import io.vertx.mutiny.core.Vertx;

import java.util.function.Consumer;

import static io.eventx.config.Constants.KUBERNETES;
import static io.eventx.config.Constants.KUBERNETES_NAMESPACE;

public class ConfigurationHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationHandler.class);
  public static final String CONFIGURATION_FORMAT = System.getenv().getOrDefault("CONFIGURATION_FORMAT", "json");

  private ConfigurationHandler() {
  }

  public static ConfigRetriever configure(Vertx vertx, String configurationName, Consumer<JsonObject> configurationConsumer) {
    final var configRetrieverOptions = new ConfigRetrieverOptions();
    if (Boolean.TRUE.equals(KUBERNETES)) {
      LOGGER.info("Kubernetes config store activated for {}", configurationName);
      ConfigStoreOptions kubernetesStore = new ConfigStoreOptions()
        .setType("configmap")
        .setFormat(CONFIGURATION_FORMAT)
        .setOptional(false)
        .setConfig(new JsonObject()
          .put("namespace", KUBERNETES_NAMESPACE)
          .put("name", configurationName)
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
      return configurationRetriever;
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
      return retriever;
    }
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

}
