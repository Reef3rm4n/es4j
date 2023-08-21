package io.es4j.infrastructure.pgbroker.core;


import io.es4j.infrastructure.pgbroker.exceptions.PartitionTakenException;
import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.Repository;
import io.es4j.task.LockLevel;
import io.es4j.task.TimerTaskConfiguration;
import io.es4j.task.TimerTaskDeployer;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class SessionManager {
    private final static Map<String, TopicPartitionPollingSession> topicPartitionSessions = new HashMap<>();
    private final QueuePollingSession queuePollingSession;
    private final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageQueue;
    private final Repository<PartitionKey, BrokerPartitionRecord, PartitionQuery> partitionRepository;
    private final String verticleId;
    private final Map<String, Instant> lastAttempt = new HashMap<>();
    private final ConsumerRouter consumerRouter;
    private final MessageRouter messageRouter;
    private final TimerTaskDeployer timerTasks;

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

    public Uni<Void> close() {
        timerTasks.close();
        if (!topicPartitionSessions.isEmpty()) {
            return Multi.createFrom().iterable(topicPartitionSessions.values())
                .onItem().transformToUniAndMerge(
                    TopicPartitionPollingSession::close
                ).collect().asList()
                .replaceWithVoid();
        }
        return Uni.createFrom().voidItem();
    }

    public SessionManager(
        final String verticleId,
        final ConsumerRouter consumerRouter,
        final Repository<MessageRecordKey, MessageRecord, MessageRecordQuery> messageQueue,
        final Repository<ConsumerFailureKey, ConsumerFailureRecord, ConsumerFailureQuery> consumerFailure,
        final Repository<PartitionKey, BrokerPartitionRecord, PartitionQuery> partitionRepository,
        final TimerTaskDeployer timerTasks
    ) {
        this.consumerRouter = consumerRouter;
        this.queuePollingSession = new QueuePollingSession(messageQueue, consumerRouter.brokerConfiguration(), verticleId);
        this.messageQueue = messageQueue;
        this.timerTasks = timerTasks;
        this.partitionRepository = partitionRepository;
        this.verticleId = verticleId;
        this.messageRouter = new MessageRouter(consumerRouter, messageQueue, consumerFailure);
    }


    public void start() {
        timerTasks.deploy(queuePollingSession.provideTask());
        timerTasks.deploy(processorTask());
    }


    private io.es4j.task.TimerTask processorTask() {
        return new io.es4j.task.TimerTask() {
            @Override
            public Uni<Void> performTask() {
                final var unis = new ArrayList<Uni<Void>>();
                if (!topicPartitionSessions.isEmpty()) {
                    unis.add(Multi.createFrom().iterable(topicPartitionSessions.entrySet())
                        .onItem().transformToUniAndMerge(partitionPollingSession -> {
                                final var topicPartitionMessages = partitionPollingSession.getValue().flush();
                                if (!topicPartitionMessages.isEmpty()) {
                                    LOGGER.info("Processing {} messages in partition {}", topicPartitionMessages.size(), partitionPollingSession.getKey());
                                    return messageRouter.routeTopicPartition(topicPartitionMessages);
                                }
                                return Uni.createFrom().voidItem();
                            }
                        ).collect().asList()
                        .replaceWithVoid()
                    );
                }
                final var queueMessages = queuePollingSession.flush();
                if (!queueMessages.isEmpty()) {
                    LOGGER.info("Processing queue messages {}", queueMessages.size());
                    unis.add(messageRouter.routeQueues(queueMessages));
                }
                if (!unis.isEmpty()) {
                    return Uni.join().all(unis).andCollectFailures().onFailure()
                      .invoke(throwable -> LOGGER.warn("Consumer dropped exception", throwable))
                      .onFailure().recoverWithNull().replaceWithVoid();
                }
                return Uni.createFrom().voidItem();

            }

            @Override
            public TimerTaskConfiguration configuration() {
                return new TimerTaskConfiguration(
                    LockLevel.NONE,
                    Duration.ofMillis(10),
                    Duration.ofMillis(10),
                    Duration.ofMillis(10),
                    Optional.empty()
                );
            }
        };
    }


    public boolean contains(String partitionId) {
        return topicPartitionSessions.containsKey(partitionId);
    }

    public void signal(String partitionId) {
        if (partitionId.equals("none")) {
            queuePollingSession.signalMessage();
        } else if (this.contains(partitionId)) {
            lastAttempt.remove(partitionId);
            Objects.requireNonNull(topicPartitionSessions.get(partitionId)).poll();
        } else {
            startPartitionSession(partitionId);
        }
    }

    private void startPartitionSession(String partitionId) {
        if (lastAttempt.containsKey(partitionId)) {
            final var lastRetry = lastAttempt.get(partitionId);
            LOGGER.debug("Partition was previously taken by another verticle, last claim attempt {}", lastRetry);
            final var has5MinutesPassedSinceLasAttempt = Instant.now().isAfter(lastRetry.plus(5, ChronoUnit.MINUTES));
            if (has5MinutesPassedSinceLasAttempt) {
                lastAttempt.put(partitionId, Instant.now());
                startSession(partitionId);
            }
        } else {
            lastAttempt.put(partitionId, Instant.now());
            startSession(partitionId);
        }
    }

    private void startSession(String partitionId) {
        LOGGER.debug("Trying to claim partition {}", partitionId);
        final var partitionPollingSession = new TopicPartitionPollingSession(partitionRepository, messageQueue, consumerRouter.brokerConfiguration(), verticleId, partitionId);
        partitionPollingSession.start(timerTasks)
            .subscribe()
            .with(item -> {
                    LOGGER.error("subscribed to partition {}", partitionId);
                    topicPartitionSessions.put(partitionId, partitionPollingSession);
                },
                throwable -> {
                    if (throwable instanceof PartitionTakenException) {
                        LOGGER.info("partition already taken {}", partitionId);
                    } else {
                        LOGGER.error("partition claiming dropped exception", throwable);
                    }
                }
            );
    }
}
