//package io.es4j.infrastructure.taskqueue;
//
//import io.smallrye.mutiny.Uni;
//
//
//
//public class MockProcessor implements MessageProcessor<MockPayload> {
//
//  @Override
//  public Uni<Void> process(MockPayload payload, QueueTransaction queueTransaction) {
//    return Uni.createFrom().voidItem();
//  }
//}
