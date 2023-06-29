package io.es4j.sql.models;

import java.util.*;

public class JsonQueryFilter<T> {

  private final Class<T> paramType;
  private String column;
  private Queue<String> jsonFields;
  private List<T> params;

  public String column() {
    return column;
  }

  public JsonQueryFilter<T> filterColumn(String column) {
    this.column = column;
    return this;
  }

  public List<T> params() {
    return params;
  }

  public JsonQueryFilter<T> filterParams(List<T> params) {
    this.params = params;
    return this;
  }
  public JsonQueryFilter<T> filterParams(T... params) {
    this.params = unpackValues(params);
    return this;
  }



  public JsonQueryFilter(Class<T> paramType) {
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

  public JsonQueryFilter validate() {
    Objects.requireNonNull(column,"Column shouldn't be null !");
    return this;
  }

  @Override
  public String toString() {
    return "QueryParam{" +
      "paramType=" + paramType +
      ", column='" + column + '\'' +
      ", params=" + params +
      ", jsonParams=" + jsonFields +
      '}';
  }

  public Queue<String> jsonFields() {
    return jsonFields;
  }

  public JsonQueryFilter<T> navJson(String jsonField) {
    if (jsonFields == null) {
      this.jsonFields = new ArrayDeque<>();
    }
    jsonFields.add(jsonField);
    return this;
  }

}
