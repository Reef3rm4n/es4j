package io.es4j.infrastructure.pgbroker;


import io.es4j.infrastructure.misc.Es4jServiceLoader;
import io.es4j.infrastructure.pgbroker.mappers.DeadLetterMapper;
import io.es4j.infrastructure.pgbroker.mappers.MessageQueueMapper;
import io.es4j.infrastructure.pgbroker.mappers.QueuePartitionMapper;
import io.es4j.infrastructure.pgbroker.messagebroker.PgChannel;
import io.es4j.infrastructure.pgbroker.models.ConsumerManager;
import io.es4j.infrastructure.pgbroker.models.ConsumerWrap;
import io.es4j.infrastructure.pgbroker.models.PgBrokerConfiguration;
import io.es4j.sql.Repository;
import io.es4j.sql.RepositoryHandler;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class PgBrokerVerticle extends AbstractVerticle {

  private final PgBrokerConfiguration configuration;
  private PgChannel pgChannel;
  private RepositoryHandler repositoryHandler;
  private static final Logger LOGGER = LoggerFactory.getLogger(PgBrokerVerticle.class);

  public PgBrokerVerticle(PgBrokerConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Uni<Void> asyncStart() {
    this.repositoryHandler = RepositoryHandler.leasePool(config(), vertx);
    this.pgChannel = createChannel();
    LOGGER.info("starting pg broker {}", deploymentID());
    return deploy();
  }

  @Override
  public Uni<Void> asyncStop() {
    return pgChannel.stop()
      .flatMap(avoid -> repositoryHandler.close())
      .replaceWithVoid();
  }

  private final String deploymentID = UUID.randomUUID().toString();

  @Override
  public String deploymentID() {
    return deploymentID;
  }

  public Uni<Void> deploy() {
    try {
      final var consumers = ServiceLoader.load(QueueConsumer.class).stream().map(ServiceLoader.Provider::get).toList();
      final var consumerTransactionProvider = configuration.consumerTransactionProvider();
      consumerTransactionProvider.start(repositoryHandler);
      return Multi.createFrom().iterable(consumers)
        .onItem().transformToUniAndMerge(m -> m.start(vertx, config()))
        .collect().asList()
        .replaceWithVoid()
        .flatMap(avoid -> pgChannel.start(
          new ConsumerManager(
            configuration,
            wrap(deploymentID, consumers),
            consumerTransactionProvider,
            repositoryHandler.vertx()
          )));

    } catch (Exception e) {
      return Uni.createFrom().failure(e);
    }
  }

  private PgChannel createChannel() {
    final var infraPgSubscriber = io.vertx.mutiny.pgclient.pubsub.PgSubscriber.subscriber(
      repositoryHandler.vertx(),
      RepositoryHandler.connectionOptions(repositoryHandler.configuration())
    );
    infraPgSubscriber.reconnectPolicy(integer -> 0L);
    return new PgChannel(
      new Repository<>(MessageQueueMapper.INSTANCE, repositoryHandler),
      new Repository<>(DeadLetterMapper.INSTANCE, repositoryHandler),
      new Repository<>(QueuePartitionMapper.INSTANCE, repositoryHandler),
      infraPgSubscriber,
      Objects.requireNonNullElse(repositoryHandler.vertx().getOrCreateContext().deploymentID(), UUID.randomUUID().toString())
    );
  }

  public List<ConsumerWrap> wrap(String deploymentId, List<QueueConsumer> queueConsumers) {
    final var queueMap = queueConsumers.stream()
      .map(impl -> {
        Class<?> firstGenericType = Es4jServiceLoader.getFirstGenericType(impl);
        return ImmutablePair.of(firstGenericType, impl);
      })
      .collect(Collectors.groupingBy(ImmutablePair::getLeft, Collectors.mapping(ImmutablePair::getRight, Collectors.toList())));
    return queueMap.entrySet().stream()
      .map(entry -> {
          final var tClass = entry.getKey();
          validateProcessors(entry.getValue(), tClass);
          final var defaultProcessor = entry.getValue().stream().filter(p -> p.tenants() == null).findFirst().orElseThrow();
          final var customProcessors = entry.getValue().stream()
            .filter(p -> p.tenants() != null)
            .collect(groupingBy(QueueConsumer::tenants));
          final var processorWrapper = new ConsumerWrap(
            deploymentId,
            defaultProcessor,
            customProcessors,
            tClass
          );
          return processorWrapper;
        }
      )
      .toList();
  }

  private static void validateProcessors(List<QueueConsumer> queues, Class<?> tClass) {
    if (queues.stream().filter(
      p -> p.tenants() == null
    ).toList().size() > 1) {
      throw new IllegalStateException("More than one default implementation for -> " + tClass);
    }
    queues.stream()
      .filter(p -> p.tenants() != null)
      .collect(groupingBy(QueueConsumer::tenants))
      .forEach((key, value) -> {
          if (value.size() > 1) {
            throw new IllegalStateException("More than one custom implementation for tenant " + key + " queue -> " + tClass);
          }
        }
      );
  }
}
