package io.vertx.skeleton.sql.models;

import java.util.*;

public class JsonQueryParams<T> {

  private final Class<T> paramType;
  private String column;
  private Queue<String> jsonFields;
  private List<T> params;

  public String column() {
    return column;
  }

  public JsonQueryParams<T> setColumn(String column) {
    this.column = column;
    return this;
  }

  public List<T> params() {
    return params;
  }

  public JsonQueryParams<T> setParams(List<T> params) {
    this.params = params;
    return this;
  }
  public JsonQueryParams<T> setParams(T... params) {
    this.params = unpackValues(params);
    return this;
  }



  public JsonQueryParams(Class<T> paramType) {
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

  public JsonQueryParams validate() {
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

  public JsonQueryParams<T> addJsonFields(String jsonField) {
    if (jsonFields == null) {
      this.jsonFields = new ArrayDeque<>();
    }
    jsonFields.add(jsonField);
    return this;
  }

}
