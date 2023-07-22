package io.es4j.infrastructure.pgbroker.messagebroker;


import io.es4j.infrastructure.pgbroker.models.*;
import io.es4j.sql.Repository;
import io.es4j.sql.misc.Constants;
import io.es4j.sql.misc.EnvVars;
import io.es4j.task.TimerTaskDeployer;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class PgChannel  {
  private static final Logger LOGGER = LoggerFactory.getLogger(PgChannel.class);
    public static final AtomicBoolean REFRESHER_DEPLOYED = new AtomicBoolean(false);
    private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue;
    private final Repository<DeadLetterKey, DeadLetterRecord, MessageRecordQuery> deadLetterQueue;
    private final io.vertx.mutiny.pgclient.pubsub.PgSubscriber pgSubscriber;
    private final Repository<PartitionKey, MessagePartition, PartitionQuery> partitionRepository;
    private final String verticleId;
    private TimerTaskDeployer timerTasks;
    private SessionManager sessionManager;


    public PgChannel(
        Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue,
        Repository<DeadLetterKey, DeadLetterRecord, MessageRecordQuery> deadLetterQueue,
        Repository<PartitionKey, MessagePartition, PartitionQuery> partitionRepository,
        io.vertx.mutiny.pgclient.pubsub.PgSubscriber pgSubscriber,
        String verticleId
    ) {
        this.messageQueue = messageQueue;
        this.deadLetterQueue = deadLetterQueue;
        this.pgSubscriber = pgSubscriber;
        this.partitionRepository = partitionRepository;
        this.verticleId = verticleId;
    }

    public Uni<Void> stop() {
        return sessionManager.close().flatMap(avoid -> pgSubscriber.close());
    }

    public Uni<Void> start(ConsumerManager consumerManager) {
        this.timerTasks = new TimerTaskDeployer(messageQueue.repositoryHandler().vertx());
        if (!REFRESHER_DEPLOYED.get()) {
            SessionRefresher.refreshTimers(consumerManager, timerTasks, messageQueue);
        }
        return PartitionHashRing.populateHashRing(partitionRepository)
            .flatMap(avoid -> {
                this.sessionManager = new SessionManager(
                    verticleId,
                    consumerManager,
                    messageQueue,
                    deadLetterQueue,
                    partitionRepository,
                    timerTasks
                );
                sessionManager.start();
                final var pgChannel = pgSubscriber.channel(parseChannel());
                pgChannel.handler(
                        partitionId -> {
                            LOGGER.info("Incoming message for partition {}", partitionId);
                            if (partitionId.isEmpty()) {
                                throw new IllegalArgumentException("partition not present in channel message");
                            }
                            sessionManager.signal(partitionId);
                        })
                    .endHandler(() -> LOGGER.info("channel stopped"))
                    .subscribeHandler(() -> LOGGER.info("channel started"))
                    .exceptionHandler(throwable -> LOGGER.error("channel error", throwable));
                return pgSubscriber.connect();
            });
    }

    private String parseChannel() {
        return messageQueue.repositoryHandler().configuration().getString(Constants.SCHEMA, EnvVars.SCHEMA) + "-queue-channel";
    }


}
