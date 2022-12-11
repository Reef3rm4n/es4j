package io.vertx.skeleton.orm.mappers;

import io.vertx.skeleton.models.*;
import io.vertx.skeleton.orm.Constants;
import io.vertx.skeleton.orm.RepositoryMapper;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;
import io.vertx.skeleton.models.Error;

import java.util.*;

public class EventJournalMapper implements RepositoryMapper<EntityEventKey, EntityEvent, EventJournalQuery> {
  private final String tableName;

  public EventJournalMapper(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public String table() {
    return tableName;
  }

  @Override
  public Set<String> insertColumns() {
    return Set.of(
      Constants.ENTITY_ID,
      Constants.EVENT,
      Constants.EVENT_VERSION,
      Constants.EVENT_CLASS,
      Constants.COMMAND,
      Constants.COMMAND_CLASS
    );
  }

  @Override
  public Set<String> updateColumns() {
    throw new OrmGenericException(new Error("Update not iplemented", "EventJournal is an append only log !", 500));
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(Constants.ENTITY_ID, Constants.EVENT_VERSION, Constants.TENANT);
  }

  @Override
  public List<Tuple2<String, List<?>>> queryFieldsColumn(EventJournalQuery queryFilter) {
    return List.of(
      Tuple2.of(Constants.ENTITY_ID, queryFilter.entityId()),
      Tuple2.of(Constants.EVENT_CLASS, queryFilter.eventType())
    );
  }

  @Override
  public void queryExtraFilters(final EventJournalQuery queryFilter, final StringJoiner stringJoiner) {
    if (queryFilter.eventVersionFrom() != null) {
      stringJoiner.add(" event_version > " + queryFilter.eventVersionFrom() + " ");
    }
  }

  @Override
  public RowMapper<EntityEvent> rowMapper() {
    return RowMapper.newInstance(
      row -> new EntityEvent(
        row.getString(Constants.ENTITY_ID),
        row.getString(Constants.EVENT_CLASS),
        row.getLong(Constants.EVENT_VERSION),
        row.getJsonObject(Constants.EVENT),
        row.getJsonObject(Constants.COMMAND),
        row.getString(Constants.COMMAND_CLASS),
        from(row)
      )
    );
  }

  @Override
  public TupleMapper<EntityEvent> tupleMapper() {
    return TupleMapper.mapper(
      entityEvent -> {
        Map<String, Object> parameters = entityEvent.persistedRecord().params();
        parameters.put(Constants.ENTITY_ID, entityEvent.entityId());
        parameters.put(Constants.EVENT, entityEvent.event());
        parameters.put(Constants.EVENT_CLASS, entityEvent.eventClass());
        parameters.put(Constants.EVENT_VERSION, entityEvent.eventVersion());
        parameters.put(Constants.COMMAND, entityEvent.command());
        parameters.put(Constants.COMMAND_CLASS, entityEvent.commandClass());
        return parameters;
      }
    );
  }

  @Override
  public TupleMapper<EntityEventKey> keyMapper() {
    return TupleMapper.mapper(
      key -> {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(Constants.EVENT_VERSION, key.eventVersion());
        parameters.put(Constants.ENTITY_ID, key.entityId());
        parameters.put(Constants.TENANT, key.tenant().generateString());
        return parameters;
      }
    );
  }

  @Override
  public Class<EntityEvent> valueClass() {
    return EntityEvent.class;
  }

  @Override
  public Class<EntityEventKey> keyClass() {
    return EntityEventKey.class;
  }

}
