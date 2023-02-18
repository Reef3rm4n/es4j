package io.vertx.skeleton.sql;

import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.skeleton.sql.commands.*;

import java.util.Map;

public interface QueryGenerator {

    String updateByKey(GenerateUpdateByKeyStatement generateUpdateByKeyStatement);
    String insert(GenerateInsertStatement generateInsertStatement);
    String selectByKey(GenerateSelectByKeyStatement generateSelectByKeyStatement);
    String selectAll(String table);
    String deleteByKey(GenerateDeleteByKeyStatement generateDeleteByKeyStatement);
    Tuple2<String, Map<String,Object>> query(GenerateQueryStatement generateQueryStatement);


}
