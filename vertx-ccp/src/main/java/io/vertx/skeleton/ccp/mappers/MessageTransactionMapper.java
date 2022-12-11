package io.vertx.skeleton.ccp.mappers;

import io.vertx.skeleton.ccp.models.*;
import io.vertx.skeleton.orm.Constants;
import io.vertx.skeleton.orm.RepositoryMapper;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.vertx.skeleton.orm.Constants.*;
import static io.vertx.skeleton.orm.mappers.Constants.*;

public class MessageTransactionMapper implements RepositoryMapper<MessageTransactionID, MessageTransaction, MessageTransactionQuery> {
  private final String tableName;
  public final RowMapper<MessageTransaction> ROW_MAPPER = RowMapper.newInstance(
    row -> new MessageTransaction(
      row.getString(MESSAGE_ID),
      row.getString(PROCESSOR),
      ProcessorType.valueOf(row.getString(TYPE)),
      from(row)
    )
  );

  public static final TupleMapper<MessageTransaction> TUPLE_MAPPER = TupleMapper.mapper(
    messageTransaction -> {
      Map<String, Object> parameters = messageTransaction.persistedRecord().params();
      parameters.put(MESSAGE_ID, messageTransaction.id());
      parameters.put(PROCESSOR, messageTransaction.processorClass());
      parameters.put(TYPE, messageTransaction.type().name());
      return parameters;
    }
  );

  public static final TupleMapper<MessageTransactionID> KEY_MAPPER = TupleMapper.mapper(
    key -> {
      Map<String, Object> parameters = new HashMap<>();
      parameters.put(MESSAGE_ID, key.messageId());
      parameters.put(TENANT, key.tenant().generateString());
      parameters.put(PROCESSOR, key.processorClass());
      return parameters;
    }
  );

  public MessageTransactionMapper(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public String table() {
    return tableName;
  }

  @Override
  public Set<String> insertColumns() {
    return Set.of(MESSAGE_ID,PROCESSOR,TYPE);
  }

  @Override
  public Set<String> updateColumns() {
    return Set.of();
  }

  @Override
  public Set<String> keyColumns() {
    return Set.of(MESSAGE_ID, PROCESSOR, Constants.TENANT);
  }

  @Override
  public RowMapper<MessageTransaction> rowMapper() {
    return ROW_MAPPER;
  }

  @Override
  public TupleMapper<MessageTransaction> tupleMapper() {
    return TUPLE_MAPPER;
  }

  @Override
  public TupleMapper<MessageTransactionID> keyMapper() {
    return KEY_MAPPER;
  }

  @Override
  public Class<MessageTransaction> valueClass() {
    return MessageTransaction.class;
  }

  @Override
  public Class<MessageTransactionID> keyClass() {
    return MessageTransactionID.class;
  }

  @Override
  public List<Tuple2<String, List<?>>> queryFieldsColumn(MessageTransactionQuery queryFilter) {
    return List.of(
      Tuple2.of(MESSAGE_ID, queryFilter.ids()),
      Tuple2.of(PROCESSOR, queryFilter.processors()),
      Tuple2.of(TYPE, queryFilter.types())
    );
  }
}
