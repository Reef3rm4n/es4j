package io.eventx.sql.generator.filters;

import io.eventx.sql.exceptions.UnmanagedQueryParam;
import io.smallrye.mutiny.tuples.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class SimpleFilter {

    private SimpleFilter(){}

    public static void addEqFilter(StringJoiner queryFilters, Map<String, Object> paramMap, Tuple2<String, List<?>> tuple) {
        if (tuple.getItem2() != null && !tuple.getItem2().isEmpty()) {
            if (tuple.getItem2().stream().anyMatch(Integer.class::isInstance)) {
                compareInt(tuple.getItem1(), tuple.getItem2().stream().map(Integer.class::cast).toList(), paramMap, queryFilters);
            }else if (tuple.getItem2().stream().anyMatch(String.class::isInstance)) {
                eqString(tuple.getItem1(), tuple.getItem2().stream().map(String.class::cast).toList(), paramMap, queryFilters);
            }else if (tuple.getItem2().stream().anyMatch(Enum.class::isInstance)) {
                compareEnum(tuple.getItem1(), tuple.getItem2().stream().map(Enum.class::cast).toList(), paramMap, queryFilters);
            }else if (tuple.getItem2().stream().anyMatch(Long.class::isInstance)) {
                compareLong(tuple.getItem1(), tuple.getItem2().stream().map(Long.class::cast).toList(), paramMap, queryFilters);
            } else {
              throw UnmanagedQueryParam.unmanagedParams(tuple.getItem2());
            }
        }
    }

    public static void addLikeFilter(StringJoiner queryFilters, Map<String, Object> paramMap, Tuple2<String, List<?>> tuple) {
        if (tuple.getItem2() != null && !tuple.getItem2().isEmpty()) {
            if (tuple.getItem2().stream().anyMatch(Integer.class::isInstance)) {
                compareInt(tuple.getItem1(), tuple.getItem2().stream().map(Integer.class::cast).toList(), paramMap, queryFilters);
            }else if (tuple.getItem2().stream().anyMatch(String.class::isInstance)) {
                likeString(tuple.getItem1(), tuple.getItem2().stream().map(String.class::cast).toList(), paramMap, queryFilters);
            }else if (tuple.getItem2().stream().anyMatch(Enum.class::isInstance)) {
                compareEnum(tuple.getItem1(), tuple.getItem2().stream().map(Enum.class::cast).toList(), paramMap, queryFilters);
            } else if (tuple.getItem2().stream().anyMatch(Long.class::isInstance)) {
                compareLong(tuple.getItem1(), tuple.getItem2().stream().map(Long.class::cast).toList(), paramMap, queryFilters);
            } else {
              throw UnmanagedQueryParam.unmanagedParams(tuple.getItem2());
            }
        }
    }
    public static void addIlikeFilter(StringJoiner queryFilters, Map<String, Object> paramMap, Tuple2<String, List<?>> tuple) {
        if (tuple.getItem2() != null && !tuple.getItem2().isEmpty()) {
            if (tuple.getItem2().stream().anyMatch(Integer.class::isInstance)) {
                compareInt(tuple.getItem1(), tuple.getItem2().stream().map(Integer.class::cast).toList(), paramMap, queryFilters);
            } else if (tuple.getItem2().stream().anyMatch(String.class::isInstance)) {
                ilikeString(tuple.getItem1(), tuple.getItem2().stream().map(String.class::cast).toList(), paramMap, queryFilters);
            }else if (tuple.getItem2().stream().anyMatch(Enum.class::isInstance)) {
                compareEnum(tuple.getItem1(), tuple.getItem2().stream().map(Enum.class::cast).toList(), paramMap, queryFilters);
            } else if (tuple.getItem2().stream().anyMatch(Long.class::isInstance)) {
                compareLong(tuple.getItem1(), tuple.getItem2().stream().map(Long.class::cast).toList(), paramMap, queryFilters);
            } else {
              throw UnmanagedQueryParam.unmanagedParams(tuple.getItem2());
            }
        }
    }

    public static void likeString(String column, List<String> params, Map<String, Object> paramMap, StringJoiner queryString) {
        if (params != null && !params.isEmpty()) {
            final var array = params.stream().map(s -> s.replace("*", "%")).toArray(String[]::new);
            paramMap.put(column, array);
            queryString.add(" " + column + " like any(#{" + column + "}) ");
        }
    }

    public static void eqString(String column, List<String> params, Map<String, Object> paramMap, StringJoiner queryString) {
        if (params != null && !params.isEmpty()) {
            final var array = params.stream().map(s -> s.replace("*", "%")).toArray(String[]::new);
            paramMap.put(column, array);
            queryString.add(" " + column + " = any(#{" + column + "}) ");
        }
    }

    public static void ilikeString(String column, List<String> params, Map<String, Object> paramMap, StringJoiner queryString) {
        if (params != null && !params.isEmpty()) {
            final var array = params.stream().map(s -> s.replace("*", "%")).toArray(String[]::new);
            paramMap.put(column, array);
            queryString.add(" " + column + " ilike any(#{" + column + "}) ");
        }
    }

    public static void compareEnum(String column, List<Enum> params, Map<String, Object> paramMap, StringJoiner queryString) {
        if (params != null && !params.isEmpty()) {
            final var array = params.stream().map(Enum::name).toArray(String[]::new);
            paramMap.put(column, array);
            queryString.add(" " + column + " = any(#{" + column + "}) ");
        }
    }

    public static void compareInt(String column, List<Integer> params, Map<String, Object> paramMap, StringJoiner queryString) {
        if (params != null && !params.isEmpty()) {
            final var array = params.toArray(Integer[]::new);
            paramMap.put(column, array);
            queryString.add(" " + column + " = any(#{" + column + "}) ");
        }
    }

    public static void compareLong(String column, List<Long> params, Map<String, Object> paramMap, StringJoiner queryString) {
        if (params != null && !params.isEmpty()) {
            final var tupleArray = params.toArray(Long[]::new);
            paramMap.put(column, tupleArray);
            queryString.add(" " + column + " = any(#{" + column + "}) ");
        }
    }
}
