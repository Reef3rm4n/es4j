package io.eventx.sql;

import io.eventx.sql.commands.*;
import io.eventx.sql.generator.PostgresQueryGenerator;
import io.eventx.sql.generator.filters.QueryBuilder;
import io.eventx.sql.misc.Constants;
import io.eventx.sql.misc.TypeExtractor;
import io.eventx.sql.models.Query;
import io.eventx.sql.models.RepositoryRecord;
import io.eventx.sql.models.RepositoryRecordKey;
import io.smallrye.mutiny.tuples.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.templates.RowMapper;
import io.vertx.mutiny.sqlclient.templates.TupleMapper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;


public class QueryGeneratorMapper<K extends RepositoryRecordKey, V extends RepositoryRecord<V>, Q extends Query> {

    public final String selectAllStatement;
    public final Class<V> actualRecordType;
    public final String selectByKeyStatement;
    public final String updateByKeyStatement;
    public final String insertStatement;
    public final String deleteByKeyStatement;
    private final Logger logger = LoggerFactory.getLogger(QueryGeneratorMapper.class);

    public final RecordMapper<K, V, Q> mapper;
    public final RowMapper<Integer> rowCounterMapper = RowMapper.newInstance(row -> row.getInteger("count"));
    public final RowMapper<V> recordRowMapper;
    public final TupleMapper<V> recordTupleMapper;
    public final TupleMapper<K> keyTupleMapper;

    public QueryGeneratorMapper(RecordMapper<K, V, Q> mapper) {
        final var keys = new HashSet<>(mapper.keyColumns());
        keys.add("tenant");
        this.mapper = mapper;
        this.selectAllStatement = PostgresQueryGenerator.INSTANCE.selectAll(mapper.table());
        this.selectByKeyStatement = PostgresQueryGenerator.INSTANCE.selectByKey(new GenerateSelectByKeyStatement(mapper.table(), keys));
        this.updateByKeyStatement = PostgresQueryGenerator.INSTANCE.updateByKey(new GenerateUpdateByKeyStatement(mapper.table(), keys, mapper.updatableColumns()));
        this.insertStatement = PostgresQueryGenerator.INSTANCE.insert(new GenerateInsertStatement(mapper.table(), mapper.columns()));
        this.deleteByKeyStatement = PostgresQueryGenerator.INSTANCE.deleteByKey(new GenerateDeleteByKeyStatement(mapper.table(), keys));
        this.recordTupleMapper = TupleMapper.mapper(
            object -> {
                Map<String, Object> parameters = object.baseRecord().params();
                mapper.params(parameters, object);
                return parameters;
            }
        );
        this.keyTupleMapper = TupleMapper.mapper(
            object -> {
                Map<String, Object> parameters = new HashMap<>();
                parameters.put(Constants.TENANT, object.tenantId());
                mapper.keyParams(parameters, object);
                return parameters;
            }
        );
        this.recordRowMapper = RowMapper.newInstance(mapper::rowMapper);
        this.actualRecordType = actualGenericTypefromInterface(mapper);
    }

    public Tuple2<String, Map<String, Object>> generateQuery(GenerateQueryCommand<Q> command) {
        final var queryBuilder = new QueryBuilder();
        mapper.queryBuilder(command.query(), queryBuilder);
        final var resultTuple = PostgresQueryGenerator.INSTANCE.query(new GenerateQueryStatement(mapper.table(), command.type(), queryBuilder.filters(), command.query().options()));
        logGeneratedQuery(resultTuple);
        return resultTuple;
    }

    private void logGeneratedQuery(Tuple2<String, Map<String, Object>> resultTuple) {
        logger.debug(
            "Resulting Query -> " +
                new JsonObject()
                    .put("type", resultTuple.getItem1())
                    .put("parameters", resultTuple.getItem2())
                    .encodePrettily()
        );
    }

    private Class<V> actualGenericTypefromInterface(RecordMapper<K, V, Q> mapper) {
        return (Class<V>) TypeExtractor.getActualGenericTypefromInterface(mapper, 1);
    }

}
