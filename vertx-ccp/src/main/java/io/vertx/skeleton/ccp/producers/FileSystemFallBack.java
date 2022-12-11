package io.vertx.skeleton.ccp.producers;

import io.vertx.skeleton.ccp.models.MessageRecord;
import io.vertx.skeleton.ccp.models.MessageRecordID;
import io.vertx.skeleton.ccp.models.MessageRecordQuery;
import io.vertx.skeleton.orm.Repository;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

import java.util.List;

public class FileSystemFallBack<T> {
  private final Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue;

  public FileSystemFallBack(Repository<MessageRecordID, MessageRecord, MessageRecordQuery> messageQueue) {
    this.messageQueue = messageQueue;
  }

  public Uni<Void> offload() {
    return messageQueue.repositoryHandler().vertx().fileSystem().readDir(messageQueue.mapper().table())
      .onItem().transformToMulti(messageIDs -> Multi.createFrom().iterable(messageIDs))
      .onItem().transformToUniAndMerge(messageID -> messageQueue.repositoryHandler().vertx().fileSystem().readFile(messageID)
        .flatMap(buffer -> {
          final var messageRecord = new JsonObject(buffer.getDelegate()).mapTo(MessageRecord.class);
          return messageQueue.transaction(
            sqlConnection -> messageQueue.insert(messageRecord)
              .flatMap(avoid -> messageQueue.repositoryHandler().vertx().fileSystem().delete(messageID))
          );
        })
      ).collect().asList()
      .replaceWithVoid();
  }

  public Uni<Void> load(MessageRecord messageRecord) {
    return messageQueue.repositoryHandler().vertx().fileSystem().writeFile(
      filePath(messageRecord),
      Buffer.newInstance(JsonObject.mapFrom(messageRecord).toBuffer())
    );
  }

  public Uni<Void> load(List<MessageRecord> messages) {
    return Multi.createFrom().iterable(messages)
      .onItem().transformToUniAndMerge(messageRecord -> messageQueue.repositoryHandler().vertx().fileSystem().writeFile(
          filePath(messageRecord),
          Buffer.newInstance(JsonObject.mapFrom(messageRecord).toBuffer())
        )
      )
      .collect().last()
      .replaceWithVoid();
  }

  private String filePath(MessageRecord messageRecord) {
    return messageQueue.mapper().table() + "/" + messageRecord.id() + ".json";
  }
}
