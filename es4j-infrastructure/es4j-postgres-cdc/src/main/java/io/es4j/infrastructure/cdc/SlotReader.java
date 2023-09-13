package io.es4j.infrastructure.cdc;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.es4j.infrastructure.cdc.models.SlotMessage;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SlotReader {

  protected static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger logger = LoggerFactory.getLogger(SlotReader.class);
  private static final String recoveryModeSqlState = "57P03";
  private static final int recoveryModeSleepMillis = 5000;
  private final PostgresConfiguration postgresConfiguration;
  private final ReplicationConfiguration replicationConfiguration;
  private final List<EventHandler> eventHandlers;
  private long lastFlushedTime;

  public SlotReader(
    final PostgresConfiguration postgresConfigurationInput,
    final ReplicationConfiguration replicationConfigurationInput
  ) {
    this.postgresConfiguration = postgresConfigurationInput;
    this.replicationConfiguration = replicationConfigurationInput;
    this.eventHandlers = ServiceLoader.load(EventHandler.class).stream().map(ServiceLoader.Provider::get).toList();
  }

  /**
   * Runs {@link #readSlot()} continuously in a loop.
   */
  public void runLoop() {
    eventHandlers.forEach(EventHandler::start);
    while (true) {
      readSlot();
    }
  }

  void readSlot() {
    try (PostgresConnector postgresConnector = createPostgresConnector(postgresConfiguration, replicationConfiguration)) {
      resetIdleCounter();
      logger.info("Consuming from slot {}", replicationConfiguration.getSlotName());
      while (true) {
        process(postgresConnector);
      }
    } catch (SQLException sqlException) {
      logger.error("Received the following error pertaining to the replication stream, reattempting...", sqlException);
      if (sqlException.getSQLState().equals(recoveryModeSqlState)) {
        logger.info("Sleeping for five seconds");
        try {
          Thread.sleep(recoveryModeSleepMillis);
        } catch (InterruptedException ie) {
          logger.error("Interrupted while sleeping", ie);
        }
      }
    } catch (IOException ioException) {
      logger.error("Received an IO Exception while processing the replication stream, reattempting...", ioException);
    } catch (Exception e) {
      logger.error("Received exception of type {}", e.getClass(), e);
    }
  }

  void process(final PostgresConnector postgresConnector) throws SQLException, IOException {
    ByteBuffer msg = postgresConnector.readPending();
    if (msg != null) {
      processByteBuffer(msg, postgresConnector);
    } else if (System.currentTimeMillis() - lastFlushedTime > TimeUnit.SECONDS.toMillis(replicationConfiguration.getUpdateIdleSlotInterval())) {
      LogSequenceNumber currentLSN = postgresConnector.getCurrentLSN();
      msg = postgresConnector.readPending();
      if (msg != null) {
        processByteBuffer(msg, postgresConnector);
      }
      logger.info("Fast forwarding stream lsn to {} due to stream inactivity", currentLSN.toString());
      postgresConnector.setStreamLsn(currentLSN);
      resetIdleCounter();
    }
  }

  void processByteBuffer(final ByteBuffer msg, final PostgresConnector postgresConnector) throws IOException {
    logger.debug("Processing chunk from wal");
    int offset = msg.arrayOffset();
    byte[] source = msg.array();
    final var slotMessage = parseSlotMessage(source, offset);
    if (!slotMessage.getChange().isEmpty()) {
      logger.info("Routing changes in slot");
      slotMessage.getChange().parallelStream().forEach(
        change -> {
          final var interestedHandlers = eventHandlers.stream().filter(eventHandler -> eventHandler.schema().equals(change.getSchema()) && eventHandler.table().equals(change.getTable())).toList();
          logger.info("Consuming change={} handlers={}", change, interestedHandlers.stream().map(h -> h.getClass().getName()).toList());
          interestedHandlers.forEach(eventHandler -> eventHandler.consume(change));
        }
      );
    }
    postgresConnector.setStreamLsn(postgresConnector.getLastReceivedLsn());
    resetIdleCounter();
    logger.info("No changes in the slot");
  }

  public void resetIdleCounter() {
    lastFlushedTime = System.currentTimeMillis();
  }

  SlotMessage parseSlotMessage(final byte[] walChunk, final int offset) throws IOException {
    return objectMapper.readValue(walChunk, offset, walChunk.length, SlotMessage.class);
  }

  PostgresConnector createPostgresConnector(final PostgresConfiguration pc, final ReplicationConfiguration rc) throws SQLException {
    return new PostgresConnector(pc, rc);
  }

}

