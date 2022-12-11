package io.vertx.skeleton.rabbitmq;

import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;

public class RabbitMQClientBootstrap {


  public static RabbitMQClient bootstrap(Vertx vertx, JsonObject configuration) {
    final var config = new RabbitMQOptions();
    if (configuration.getString("rabbitUri") != null) {
      config.setUri(configuration.getString("rabbitUri"));
    }
    if (configuration.getString("rabbitmqHost") != null) {
      config.setPort(configuration.getInteger("rabbitmqPort"));
      config.setHost(configuration.getString("rabbitmqHost"));
    }
    config.setReusePort(true)
      .setUseAlpn(true)
      .setTcpCork(true)
      .setTcpFastOpen(true)
      .setTcpNoDelay(true)
      .setTcpQuickAck(true);
    config.setIncludeProperties(true);
    if (configuration.getString("rabbitmqUser") != null) {
      config.setUser(configuration.getString("rabbitmqUser"));
      config.setPassword(configuration.getString("rabbitmqPassword"));
    }
    return RabbitMQClient.create(vertx, config);
  }

}
