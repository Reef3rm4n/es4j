//package io.es4j.infrastructure.taskqueue;
//
//import io.smallrye.mutiny.Uni;
//import io.es4j.queue.MessageProcessor;
//import io.es4j.queue.models.QueueTransaction;
//
//import java.util.List;
//
//public class MockDeadPayloadProcessor implements MessageProcessor<MockDeadPayload> {
//  @Override
//  public Uni<Void> process(MockDeadPayload payload, QueueTransaction queueTransaction) {
//    if (payload.fatal()) {
//      return Uni.createFrom().failure(new FatalException("Fatal failure !"));
//    }
//    return Uni.createFrom().failure(new RuntimeException("Mocking failure !"));
//  }
//
//  @Override
//  public List<Class<? extends Throwable>> fatalExceptions() {
//    return List.of(FatalException.class);
//  }
//
//
//}
