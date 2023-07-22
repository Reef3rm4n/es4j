package io.es4j.infrastructure.pgbroker.messagebroker;


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
    private final static Map<String, PartitionPollingSession> partitionedSessions = new HashMap<>();
    private final ConcurrentPollingSession concurrentSession;
    private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue;
    private final Repository<PartitionKey, MessagePartition, PartitionQuery> partitionRepository;
    private final String verticleId;
    private final Map<String, Instant> lastAttempt = new HashMap<>();
    private final ConsumerManager consumerManager;
    private final MessageProcessor messageProcessor;
    private TimerTaskDeployer timerTasks;

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

    public Uni<Void> close() {
        timerTasks.close();
        if (!partitionedSessions.isEmpty()) {
            return Multi.createFrom().iterable(partitionedSessions.values())
                .onItem().transformToUniAndMerge(
                    PartitionPollingSession::close
                ).collect().asList()
                .replaceWithVoid();
        }
        return Uni.createFrom().voidItem();
    }

    public SessionManager(
        final String verticleId,
        final ConsumerManager consumerManager,
        final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue,
        final Repository<DeadLetterKey, DeadLetterRecord, MessageRecordQuery> deadLetterQueue,
        final Repository<PartitionKey, MessagePartition, PartitionQuery> partitionRepository,
        final TimerTaskDeployer timerTasks
    ) {
        this.consumerManager = consumerManager;
        this.concurrentSession = new ConcurrentPollingSession(messageQueue, consumerManager.pgBrokerConfiguration(), verticleId);
        this.messageQueue = messageQueue;
        this.timerTasks = timerTasks;
        this.partitionRepository = partitionRepository;
        this.verticleId = verticleId;
        this.messageProcessor = new MessageProcessor(consumerManager, messageQueue, deadLetterQueue);
    }


    public void start() {
        timerTasks.deploy(concurrentSession.provideTask());
        timerTasks.deploy(processorTask());
    }


    private io.es4j.task.TimerTask processorTask() {
        return new io.es4j.task.TimerTask() {
            @Override
            public Uni<Void> performTask() {
                final var unis = new ArrayList<Uni<Void>>();
                if (!partitionedSessions.isEmpty()) {
                    unis.add(Multi.createFrom().iterable(partitionedSessions.entrySet())
                        .onItem().transformToUniAndMerge(partitionPollingSession -> {
                                final var messages = partitionPollingSession.getValue().pollMessages();
                                if (!messages.isEmpty()) {
                                    LOGGER.info("Processing {} messages in  partition {}", messages.size(), partitionPollingSession.getKey());
                                    return messageProcessor.processPartitions(messages);
                                }
                                return Uni.createFrom().voidItem();
                            }
                        ).collect().asList()
                        .replaceWithVoid()
                    );
                }
                final var messages = concurrentSession.pollMessages();
                if (!messages.isEmpty()) {
                    LOGGER.info("Processing {} messages", messages.size());
                    unis.add(messageProcessor.processConcurrent(messages));
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
        return partitionedSessions.containsKey(partitionId);
    }

    public void signal(String partitionId) {
        if (partitionId.equals("none")) {
            concurrentSession.signalMessage();
        } else if (this.contains(partitionId)) {
            lastAttempt.remove(partitionId);
            Objects.requireNonNull(partitionedSessions.get(partitionId)).signalMessage();
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
        final var partitionPollingSession = new PartitionPollingSession(partitionRepository, messageQueue, consumerManager.pgBrokerConfiguration(), verticleId, partitionId);
        partitionPollingSession.start(timerTasks)
            .subscribe()
            .with(item -> {
                    LOGGER.error("subscribed to partition {}", partitionId);
                    partitionedSessions.put(partitionId, partitionPollingSession);
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
