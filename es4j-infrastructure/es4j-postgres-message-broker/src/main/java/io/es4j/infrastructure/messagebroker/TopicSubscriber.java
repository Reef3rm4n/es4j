package io.es4j.infrastructure.messagebroker;


import io.es4j.infrastructure.messagebroker.models.MessageProcessorManager;
import io.smallrye.mutiny.Uni;

public interface TopicSubscriber {

  Uni<Void> unsubscribe();

  Uni<Void> subscribe(MessageProcessorManager messageProcessorManager);



}
