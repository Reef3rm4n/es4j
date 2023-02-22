package io.vertx.skeleton.taskqueue.models;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class TaskQueueConfiguration {

  private Boolean bootstrapQueue = true;
  private Boolean idempotency = false;
  private Integer idempotencyNumberOfDays = 2;
  private QueueImplementation queueImplementation = QueueImplementation.PG_QUEUE;

  private TransactionManagerImplementation transactionManagerImplementation = TransactionManagerImplementation.VERTX_PG_CLIENT;
  private Long emptyBackOffInMinutes = 5L;
  private Long throttleInMs = 100L;
  private Integer concurrency = null;
  private Long errorBackOffInMinutes = 5L;
  private Long retryInvervalInSeconds = 1L;
  private Long maxProcessingTimeInMinutes = 5L;
  private Long batchSize = 200L;
  private Integer maxRetry = 1;
  private Long maintenanceEvery = 30L;
  private Integer circuitBreakerMaxFailues = 10;

  public TransactionManagerImplementation transactionManagerImplementation() {
    return transactionManagerImplementation;
  }

  public TaskQueueConfiguration setTransactionManagerImplementation(TransactionManagerImplementation transactionManagerImplementation) {
    this.transactionManagerImplementation = transactionManagerImplementation;
    return this;
  }

  public Boolean idempotentProcessors() {
    return idempotency;
  }

  public Integer circuitBreakerMaxFailues() {
    return circuitBreakerMaxFailues;
  }

  public TaskQueueConfiguration setCircuitBreakerMaxFailuer(Integer circuitBreakerMaxFailuer) {
    this.circuitBreakerMaxFailues = circuitBreakerMaxFailuer;
    return this;
  }

  public Boolean bootstrapQueue() {
    return bootstrapQueue;
  }

  public TaskQueueConfiguration setBootstrapQueue(Boolean bootstrapQueue) {
    this.bootstrapQueue = bootstrapQueue;
    return this;
  }

  public Integer concurrency() {
    return concurrency;
  }

  public TaskQueueConfiguration setConcurrency(Integer concurrency) {
    this.concurrency = concurrency;
    return this;
  }

  public Long maintenanceEvery() {
    return maintenanceEvery;
  }

  public TaskQueueConfiguration setMaintenanceEvery(Long maintenanceEvery) {
    this.maintenanceEvery = maintenanceEvery;
    return this;
  }

  public Boolean idempotentProcess() {
    return idempotency;
  }

  public TaskQueueConfiguration setIdempotency(Boolean idempotency) {
    this.idempotency = idempotency;
    return this;
  }

  public Integer idempotencyNumberOfDays() {
    return idempotencyNumberOfDays;
  }

  public TaskQueueConfiguration setIdempotencyNumberOfDays(Integer idempotencyNumberOfDays) {
    this.idempotencyNumberOfDays = idempotencyNumberOfDays;
    return this;
  }
  public QueueImplementation queueImplementation() {
    return queueImplementation;
  }

  public TaskQueueConfiguration setQueueType(QueueImplementation queueImplementation) {
    this.queueImplementation = queueImplementation;
    return this;
  }

  public Long emptyBackOffInMinutes() {
    return emptyBackOffInMinutes;
  }

  public TaskQueueConfiguration setEmptyBackOffInMinutes(Long emptyBackOffInMinutes) {
    this.emptyBackOffInMinutes = emptyBackOffInMinutes;
    return this;
  }

  public Long throttleInMs() {
    return throttleInMs;
  }

  public TaskQueueConfiguration setThrottleInMs(Long throttleInMs) {
    this.throttleInMs = throttleInMs;
    return this;
  }

  public Long errorBackOffInMinutes() {
    return errorBackOffInMinutes;
  }

  public TaskQueueConfiguration setErrorBackOffInMinutes(Long errorBackOffInMinutes) {
    this.errorBackOffInMinutes = errorBackOffInMinutes;
    return this;
  }

  public Long retryIntervalInSeconds() {
    return retryInvervalInSeconds;
  }

  public TaskQueueConfiguration setRetryInvervalInSeconds(Long retryInvervalInSeconds) {
    this.retryInvervalInSeconds = retryInvervalInSeconds;
    return this;
  }

  public Long maxProcessingTimeInMinutes() {
    return maxProcessingTimeInMinutes;
  }

  public TaskQueueConfiguration setMaxProcessingTimeInMinutes(Long maxProcessingTimeInMinutes) {
    this.maxProcessingTimeInMinutes = maxProcessingTimeInMinutes;
    return this;
  }

  public Long batchSize() {
    return batchSize;
  }

  public TaskQueueConfiguration setBatchSize(Long batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public Integer maxRetry() {
    return maxRetry;
  }

  public TaskQueueConfiguration setMaxRetry(Integer maxRetry) {
    this.maxRetry = maxRetry;
    return this;
  }


}
