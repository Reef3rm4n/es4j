package io.vertx.eventx.task;

import com.cronutils.model.time.ExecutionTime;
import io.smallrye.mutiny.Uni;
import io.vertx.eventx.queue.MessageProcessor;
import io.vertx.eventx.queue.MessageProducer;
import io.vertx.eventx.queue.models.Message;
import io.vertx.eventx.queue.models.QueueTransaction;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

public class CronTaskProcessor implements MessageProcessor<CronTaskMessage> {

  private final List<CronTask> tasks;
  private final MessageProducer producer;

  public CronTaskProcessor(List<CronTask> tasks, MessageProducer producer) {
    this.tasks = tasks;
    this.producer = producer;
  }

  @Override
  public Uni<Void> process(CronTaskMessage payload, QueueTransaction queueTransaction) {
    final var task = tasks.stream().filter(t -> t.getClass().getName().equals(payload.taskClass()))
      .findFirst().orElseThrow();
    final Message<CronTaskMessage> message = taskScheduledMessage(task);
    return task.performTask(queueTransaction)
      .onFailure(throwable -> task.configuration().knownInterruptions().stream().anyMatch(t -> t.isAssignableFrom(throwable.getClass())))
      .recoverWithNull()
      .flatMap(avoid -> producer.enqueue(message, queueTransaction));
  }


  public static Message<CronTaskMessage> taskScheduledMessage(CronTask task) {
    return new Message<>(
      task.getClass().getName() + "::" + UUID.randomUUID(),
      "default",
      calculateNextExecutionTime(task),
      null,
      task.configuration().priority(),
      new CronTaskMessage(
        task.getClass().getName(),
        Instant.now()
      )
    );
  }

  private static Instant calculateNextExecutionTime(CronTask task) {
    final var executionTime = ExecutionTime.forCron(task.configuration().cron());
    return executionTime.nextExecution(ZonedDateTime.now()).orElseThrow().toInstant();
  }
}
