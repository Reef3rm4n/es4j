package io.es4j.infrastructure.pgbroker;


import io.es4j.infrastructure.misc.Es4jServiceLoader;
import io.es4j.infrastructure.pgbroker.mappers.ConsumerFailureMapper;
import io.es4j.infrastructure.pgbroker.mappers.MessageMapper;
import io.es4j.infrastructure.pgbroker.mappers.MessageTransactionMapper;
import io.es4j.infrastructure.pgbroker.mappers.BrokerPartitionMapper;
import io.es4j.infrastructure.pgbroker.core.PgChannel;
import io.es4j.infrastructure.pgbroker.models.BrokerConfiguration;
import io.es4j.infrastructure.pgbroker.models.ConsumerRouter;
import io.es4j.infrastructure.pgbroker.models.QueueConsumerWrapper;
import io.es4j.infrastructure.pgbroker.models.TopicSubscriberWrapper;
import io.es4j.sql.Repository;
import io.es4j.sql.RepositoryHandler;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;


public class PgBrokerVerticle extends AbstractVerticle {

  private final BrokerConfiguration configuration;
  private PgChannel pgChannel;
  private RepositoryHandler repositoryHandler;
  private static final Logger LOGGER = LoggerFactory.getLogger(PgBrokerVerticle.class);

  public PgBrokerVerticle(BrokerConfiguration configuration) {
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
    LOGGER.info("stopping pg broker {}", deploymentID());
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
      final var topicConsumers = ServiceLoader.load(TopicSubscription.class).stream().map(ServiceLoader.Provider::get).toList();
      final var queueConsumers = ServiceLoader.load(QueueConsumer.class).stream().map(ServiceLoader.Provider::get).toList();
      final var consumerTransactionProvider = configuration.consumerTransactionProvider();
      consumerTransactionProvider.start(repositoryHandler);
      return startTopicConsumers(topicConsumers)
        .flatMap(__ -> startQueueConsumers(queueConsumers))
        .flatMap(avoid -> pgChannel.start(
            new ConsumerRouter(
              configuration,
              wrapTopic(topicConsumers),
              wrapQueue(queueConsumers),
              consumerTransactionProvider,
              repositoryHandler.vertx()
            )
          )
        );
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
      new Repository<>(MessageTransactionMapper.INSTANCE, repositoryHandler),
      new Repository<>(MessageMapper.INSTANCE, repositoryHandler),
      new Repository<>(ConsumerFailureMapper.INSTANCE, repositoryHandler),
      new Repository<>(BrokerPartitionMapper.INSTANCE, repositoryHandler),
      infraPgSubscriber,
      deploymentID()
    );
  }

  private List<QueueConsumerWrapper> wrapQueue(List<QueueConsumer> queueConsumers) {
    return queueConsumers.stream()
      .map(impl -> {
          Class<?> firstGenericType = Es4jServiceLoader.getFirstGenericType(impl);
          return new QueueConsumerWrapper(impl, firstGenericType);
        }
      ).toList();
  }

  public List<TopicSubscriberWrapper> wrapTopic(List<TopicSubscription> consumers) {
    return consumers.stream()
      .map(impl -> {
          Class<?> firstGenericType = Es4jServiceLoader.getFirstGenericType(impl);
          final var pattern = Pattern.compile(impl.address());
          return new TopicSubscriberWrapper<>(impl, firstGenericType, pattern);
        }
      ).toList();
  }

  private Uni<Void> startQueueConsumers(List<QueueConsumer> queueConsumers) {
    return Multi.createFrom().iterable(queueConsumers)
      .onItem().transformToUniAndMerge(m -> m.start(vertx, config()))
      .collect().asList()
      .replaceWithVoid();
  }

  private Uni<Void> startTopicConsumers(List<TopicSubscription> topicConsumers) {
    return Multi.createFrom().iterable(topicConsumers)
      .onItem().transformToUniAndMerge(m -> m.start(vertx, config()))
      .collect().asList()
      .replaceWithVoid();
  }


}
