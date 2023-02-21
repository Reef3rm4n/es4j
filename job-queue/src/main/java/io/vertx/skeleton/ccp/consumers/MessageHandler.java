package io.vertx.skeleton.ccp.consumers;

import io.vertx.skeleton.ccp.models.QueueConfiguration;
import io.vertx.skeleton.ccp.models.MessageRecord;
import io.smallrye.mutiny.Uni;

import java.util.List;

public sealed interface MessageHandler permits TaskQueueHandler {

  Uni<Void> process(final List<MessageRecord> queueEntries);

  QueueConfiguration queueConfiguration();

}
