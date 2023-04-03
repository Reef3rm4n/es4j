package io.vertx.eventx.config;

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

import static io.vertx.eventx.config.Constants.KUBERNETES;
import static io.vertx.eventx.config.Constants.KUBERNETES_NAMESPACE;

public class ConfigurationHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationHandler.class);
  public static final String CONFIGURATION_FORMAT = System.getenv().getOrDefault("CONFIGURATION_FORMAT", "json");

  private ConfigurationHandler() {
  }

  public static ConfigRetriever configure(Vertx vertx, String configurationName, Consumer<JsonObject> configurationConsumer) {
    final var configRetrieverOptions = new ConfigRetrieverOptions();
    if (Boolean.TRUE.equals(KUBERNETES)) {
      LOGGER.info("Kubernetes configuration store activated for configmap -> " + configurationName);
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
          LOGGER.debug("Previous configuration -> " + configChange.getPreviousConfiguration().encodePrettily());
          LOGGER.info("New configuration -> " + configChange.getNewConfiguration().encodePrettily());
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
      LOGGER.info("File configuration store activated for file -> " + configurationName + "." + CONFIGURATION_FORMAT);
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
          avoid -> LOGGER.info("Configuration consumed by -> " + configurationConsumer.getClass()),
          throwable -> LOGGER.error("Unable to read configuration ", throwable)
        );
      return retriever;
    }
  }


  private static JsonObject parseConfiguration(final String config) {
    try {
      final var mapper = new ObjectMapper(new YAMLFactory());
      final var finalConfiguration = JsonObject.mapFrom(mapper.readValue(config, Object.class));
      LOGGER.info("Parsed configuration -> " + finalConfiguration.encodePrettily());
      return finalConfiguration;
    } catch (JsonProcessingException jsonProcessingException) {
      LOGGER.error("Unable to parse yaml", jsonProcessingException);
      throw new IllegalArgumentException(jsonProcessingException);
    }
  }

}
