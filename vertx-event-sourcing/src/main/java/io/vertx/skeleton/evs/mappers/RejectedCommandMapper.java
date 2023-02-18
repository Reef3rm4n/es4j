package io.vertx.skeleton.evs.mappers;

import io.vertx.skeleton.ccp.mappers.MessageQueueSql;
import io.vertx.skeleton.evs.EntityAggregate;
import io.vertx.skeleton.evs.objects.EntityAggregateKey;
import io.vertx.skeleton.evs.objects.RejectedCommand;
import io.vertx.skeleton.models.*;
import io.vertx.skeleton.models.exceptions.OrmGenericException;
import io.vertx.skeleton.orm.Constants;
import io.vertx.skeleton.orm.RepositoryMapper;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;
import io.vertx.skeleton.models.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RejectedCommandMapper implements RepositoryMapper<EntityAggregateKey, RejectedCommand, EmptyQuery> {
  private final String tableName;


  public <T extends EntityAggregate> RejectedCommandMapper(Class<T> entityAggregateClass) {
    this.tableName = MessageQueueSql.camelToSnake(entityAggregateClass.getSimpleName()) + "_rejected_commands";
  }

  @Override
  public String table() {
    return tableName;
  }

  @Override
  public Set<String> insertColumns() {
    return Set.of(Constants.ENTITY_ID, Constants.COMMAND, Constants.ERROR);
  }

  @Override
  public Set<String> updateColumns() {
    throw new OrmGenericException(new Error("Update not iplemented", "EventJournal is an append only log !", 500));
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(Constants.ENTITY_ID, Constants.TENANT);
  }




  @Override
  public RowMapper<RejectedCommand> rowMapper() {
    return RowMapper.newInstance(
      row -> new RejectedCommand(
        row.getString(Constants.ENTITY_ID),
        row.getJsonObject(Constants.COMMAND),
        row.getString(Constants.COMMAND_CLASS),
        row.getJsonObject(Constants.ERROR),
        from(row)
      )
    );
  }
  @Override
  public TupleMapper<RejectedCommand> tupleMapper() {
    return TupleMapper.mapper(
      rejectedCommand -> {
        Map<String, Object> parameters = rejectedCommand.persistedRecord().params();
        parameters.put(Constants.ENTITY_ID, rejectedCommand.entityId());
        parameters.put(Constants.COMMAND, rejectedCommand.command());
        parameters.put(Constants.ERROR, rejectedCommand.error());
        return parameters;
      }
    );
  }

  @Override
  public TupleMapper<EntityAggregateKey> keyMapper() {
    return TupleMapper.mapper(
      key -> {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(Constants.ENTITY_ID, key.entityId());
        parameters.put(Constants.TENANT, key.tenant().generateString());
        return parameters;
      }
    );
  }

  @Override
  public Class<RejectedCommand> valueClass() {
    return RejectedCommand.class;
  }

  @Override
  public Class<EntityAggregateKey> keyClass() {
    return EntityAggregateKey.class;
  }
}
