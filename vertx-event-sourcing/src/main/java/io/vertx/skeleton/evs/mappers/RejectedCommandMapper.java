package io.vertx.skeleton.evs.mappers;

import io.vertx.skeleton.sql.RecordMapper;
import io.vertx.skeleton.evs.objects.EntityAggregateKey;
import io.vertx.skeleton.evs.objects.RejectedCommand;
import io.vertx.skeleton.sql.generator.filters.QueryBuilder;
import io.vertx.skeleton.sql.models.EmptyQuery;
import io.vertx.sqlclient.Row;
import java.util.Map;
import java.util.Set;

public class RejectedCommandMapper implements RecordMapper<EntityAggregateKey, RejectedCommand, EmptyQuery> {


  private static final String ENTITY_ID = "entity_id";
  private static final String COMMAND = "command";
  private static final String COMMAND_CLASS = "command_class";
  private static final String ERROR = "error";

  private RejectedCommandMapper(){}
  public static RejectedCommandMapper INSTANCE = new RejectedCommandMapper();

  @Override
  public String table() {
    return "rejected_commands";
  }

  @Override
  public Set<String> columns() {
    return Set.of(ENTITY_ID, COMMAND, ERROR);
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(ENTITY_ID);
  }

  @Override
  public RejectedCommand rowMapper(Row row) {
    return new RejectedCommand(
      row.getString(ENTITY_ID),
      row.getJsonObject(COMMAND),
      row.getString(COMMAND_CLASS),
      row.getJsonObject(ERROR),
      baseRecord(row)
    );
  }

  @Override
  public void params(Map<String, Object> params, RejectedCommand actualRecord) {
    params.put(ENTITY_ID, actualRecord.entityId());
    params.put(COMMAND, actualRecord.command());
    params.put(ERROR, actualRecord.error());
  }

  @Override
  public void keyParams(Map<String, Object> params, EntityAggregateKey key) {
    params.put(ENTITY_ID, key.entityId());
  }

  @Override
  public void queryBuilder(EmptyQuery query, QueryBuilder builder) {

  }

}
