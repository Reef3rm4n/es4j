package io.es4j.saga;

import io.es4j.sql.exceptions.NotFound;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.UniHelper;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public record SagaManager<T extends SagaTrigger, R>(
  Class<R> requestClass,
  Class<T> payloadClass,
  Saga<T, R> saga,
  Set<? extends SagaTransaction<T>> transactions,
  SagaStore sagaStore
) {

  public boolean isMatch(String rClass) {
    return requestClass.getName().equals(rClass);
  }

  public T supplyPayload(JsonObject request) {
    return saga.supplyPayload(request.mapTo(requestClass));
  }

  public Uni<Void> process(JsonObject request, SagaStore sagaStore) {
    final var payload = new AtomicReference<>(saga.supplyPayload(request.mapTo(requestClass)));
    if (payload.get().fireAndForget()) {
      handleSaga(sagaStore, payload)
        .subscribe()
        .with(UniHelper.NOOP);
      return Uni.createFrom().voidItem();
    }
    return handleSaga(sagaStore, payload);
  }

  private Uni<Void> handleSaga(SagaStore sagaStore, AtomicReference<T> payload) {
    return sagaStore.fetchSaga(payload.get().id())
      .onFailure(NotFound.class).recoverWithNull().replaceWith(sagaRecord(payload))
      .flatMap(sagaRecord -> switch (sagaRecord.state()) {
        case STAGED -> Uni.createFrom().voidItem();
        case INITIALIZED -> performSaga(sagaStore, payload, sagaRecord);
        case COMMITTED -> rollback(sagaStore, payload, sagaRecord);
        default -> Uni.createFrom().failure(new IllegalStateException());
      })
      .replaceWithVoid();
  }


  private SagaRecord sagaRecord(AtomicReference<T> payload) {
    return new SagaRecord(
      payload.get().id(),
      SagaState.INITIALIZED,
      transactions.stream().map(t -> new SagaTransactionRecord(t.name(), SagaTransactionState.INITIALIZED, null)).collect(Collectors.toSet()),
      null,
      JsonObject.mapFrom(payload.get()).getMap()
    );
  }

  private Uni<?> rollback(SagaStore sagaStore, AtomicReference<T> payload, SagaRecord sagaRecord) {
    return Uni.createFrom().voidItem();
  }

  private Uni<Void> performSaga(SagaStore sagaStore, AtomicReference<T> payload, SagaRecord sagaRecord) {
    if (Objects.nonNull(sagaRecord.payload())) {
      payload.set(JsonObject.mapFrom(sagaRecord.payload()).mapTo(payloadClass));
    }
    final var executionStack = new Stack<SagaTransaction<T>>();
    final var rollbackStack = new Stack<SagaTransaction<T>>();
    transactions.stream()
      .filter(requiredTransaction -> sagaRecord.transactions().stream()
        .noneMatch(executedTransaction -> executedTransaction.transactionName().equals(requiredTransaction.name()))
      )
      .forEach(executionStack::push);
    return Multi.createFrom().iterable(executionStack)
      .onItem().transformToUniAndConcatenate(transaction -> {
          final var record = sagaRecord.transactions().stream().filter(t -> transaction.name().equals(t.transactionName())).findFirst().orElseThrow();
          return processTransaction(payload, record, transaction, sagaRecord)
            .onItemOrFailure().transformToUni(
              (item, failure) -> {
                rollbackStack.push(transaction);
                if (Objects.nonNull(failure)) {
                  // todo interrupt flow trigger rollback
                } else {

                }
                return null;
              }
            );
        }
      )
      .collect().asList()
      .onFailure().invoke(
        throwable -> {

        }
      )
      .replaceWithVoid();
  }

  private Uni<? extends T> processTransaction(AtomicReference<T> payload, SagaTransactionRecord sagaTransactionRecord, SagaTransaction<T> transaction, SagaRecord record) {
    return switch (sagaTransactionRecord.state()) {
      case INITIALIZED -> stage(record, sagaTransactionRecord, payload, transaction);
      case STAGED -> commit(record, sagaTransactionRecord, payload, transaction);
      case COMMIT_FAILURE, REVERT_FAILURE, STAGE_FAILURE -> rollback(record, sagaTransactionRecord, payload, transaction);
      case COMMITTED, REVERTED -> Uni.createFrom().item(payload.get());
    };
  }

  private Uni<T> rollback(SagaRecord sagaRecord, SagaTransactionRecord transactionRecord, AtomicReference<T> payload, SagaTransaction<T> transaction) {
    return transaction.stage(payload.get()).map(payload::getAndSet)
      .onFailure().retry().withBackOff(transaction.configuration().retryBackOff()).atMost(transaction.configuration().numberOfRetries())
      .onItemOrFailure().call((item, failure) -> handleRollbackResult(sagaRecord, transactionRecord, item, failure));
  }

  private Uni<Void> handleRollbackResult(SagaRecord sagaRecord, SagaTransactionRecord transactionRecord, T item, Throwable failure) {
    if (Objects.nonNull(failure)) {
      final var tResult = SagaTransactionRecordBuilder.builder(transactionRecord).state(SagaTransactionState.REVERT_FAILURE).build();
      sagaRecord.transactions().add(tResult);
    } else {
      final var tResult = SagaTransactionRecordBuilder.builder(transactionRecord).state(SagaTransactionState.REVERTED).build();
      sagaRecord.transactions().add(tResult);
    }
    return sagaStore.update(SagaRecordBuilder.builder(sagaRecord.computeState())
      .payload(JsonObject.mapFrom(item).getMap())
      .build());
  }

  private Uni<T> stage(SagaRecord sagaRecord, SagaTransactionRecord transactionRecord, AtomicReference<T> payload, SagaTransaction<T> transaction) {
    return transaction.stage(payload.get()).map(payload::getAndSet)
      .onItemOrFailure().call((item, failure) -> handleStageResult(sagaRecord, transactionRecord, item, failure));
  }

  private Uni<Void> handleStageResult(SagaRecord sagaRecord, SagaTransactionRecord transactionRecord, T item, Throwable failure) {
    if (Objects.nonNull(failure)) {
      final var tResult = SagaTransactionRecordBuilder.builder(transactionRecord).state(SagaTransactionState.STAGE_FAILURE).build();
      sagaRecord.transactions().add(tResult);
    } else {
      final var tResult = SagaTransactionRecordBuilder.builder(transactionRecord).state(SagaTransactionState.STAGED).build();
      sagaRecord.transactions().add(tResult);
    }
    return sagaStore.update(SagaRecordBuilder.builder(sagaRecord.computeState())
      .payload(JsonObject.mapFrom(item).getMap())
      .build());
  }

  private Uni<T> commit(SagaRecord sagaRecord, SagaTransactionRecord transactionRecord, AtomicReference<T> payload, SagaTransaction<T> transaction) {
    return transaction.commit(payload.get()).map(payload::getAndSet)
      .onItemOrFailure().call((item, failure) -> handleCommitResult(sagaRecord, transactionRecord, item, failure));
  }

  private Uni<Void> handleCommitResult(SagaRecord sagaRecord, SagaTransactionRecord transactionRecord, T item, Throwable failure) {
    if (Objects.nonNull(failure)) {
      final var tResult = SagaTransactionRecordBuilder.builder(transactionRecord).state(SagaTransactionState.COMMIT_FAILURE).build();
      sagaRecord.transactions().add(tResult);
    } else {
      final var tResult = SagaTransactionRecordBuilder.builder(transactionRecord).state(SagaTransactionState.COMMITTED).build();
      sagaRecord.transactions().add(tResult);
    }

    return sagaStore.update(SagaRecordBuilder.builder(sagaRecord.computeState())
      .payload(JsonObject.mapFrom(item).getMap())
      .build());
  }

}
