package io.es4j.sql.models;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class QueryFilters<T> {

  private final Class<T> paramType;
  private String column;
  private List<T> params;

  public String column() {
    return column;
  }

  public QueryFilters<T> filterColumn(String column) {
    this.column = column;
    return this;
  }

  public List<T> params() {
    return params;
  }

  public QueryFilters<T> filterParams(List<T> params) {
    this.params = params;
    return this;
  }

  public QueryFilters<T> filterParams(T... params) {
    this.params = unpackValues(params);
    return this;
  }

  public QueryFilters<T> filterParam(T param) {
    if (param != null) {
      this.params = List.of(param);
    }
    return this;
  }


  public QueryFilters(Class<T> paramType) {
    this.paramType = paramType;
  }

  private static <T> List<T> unpackValues(T[] values) {
    return Arrays.stream(values).map(
        value -> {
          if (value instanceof Collection collection) {
            return collection.stream().toList();
          } else return List.of(value);
        }
      )
      .flatMap(List::stream)
      .toList();
  }

  public QueryFilters validate() {
    Objects.requireNonNull(column, "Column shouldn't be null !");
    return this;
  }

  @Override
  public String toString() {
    return "QueryParam{" +
      "paramType=" + paramType +
      ", column='" + column + '\'' +
      ", params=" + params +
      '}';
  }
}
