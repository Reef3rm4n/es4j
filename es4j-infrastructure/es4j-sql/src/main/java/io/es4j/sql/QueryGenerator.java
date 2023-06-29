package io.es4j.sql;

import io.es4j.sql.commands.*;
import io.smallrye.mutiny.tuples.Tuple2;

import java.util.Map;

public interface QueryGenerator {

    String updateByKey(GenerateUpdateByKeyStatement generateUpdateByKeyStatement);
    String insert(GenerateInsertStatement generateInsertStatement);
    String selectByKey(GenerateSelectByKeyStatement generateSelectByKeyStatement);
    String selectAll(String table);
    String deleteByKey(GenerateDeleteByKeyStatement generateDeleteByKeyStatement);
    Tuple2<String, Map<String,Object>> query(GenerateQueryStatement generateQueryStatement);


}
