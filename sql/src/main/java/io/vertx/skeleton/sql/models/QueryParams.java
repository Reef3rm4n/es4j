package io.vertx.skeleton.sql.models;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class QueryParams<T> {

  private final Class<T> paramType;
  private String column;
  private List<T> params;

  public String column() {
    return column;
  }

  public QueryParams<T> setColumn(String column) {
    this.column = column;
    return this;
  }

  public List<T> params() {
    return params;
  }

  public QueryParams<T> setParams(List<T> params) {
    this.params = params;
    return this;
  }
  public QueryParams<T> setParams(T... params) {
    this.params = unpackValues(params);
    return this;
  }



  public QueryParams(Class<T> paramType) {
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

  public QueryParams validate() {
    Objects.requireNonNull(column,"Column shouldn't be null !");
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
