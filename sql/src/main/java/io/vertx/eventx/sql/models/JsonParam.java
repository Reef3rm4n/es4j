package io.vertx.eventx.sql.models;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class JsonParam<T> {

  private final Class<T> paramType;
  private String jsonField;
  private List<T> params;

  public String jsonField() {
    return jsonField;
  }

  public JsonParam<T> setJsonField(String jsonField) {
    this.jsonField = jsonField;
    return this;
  }

  public List<T> params() {
    return params;
  }

  public JsonParam<T> setParams(List<T> params) {
    this.params = params;
    return this;
  }
  public JsonParam<T> setParams(T... params) {
    this.params = unpackValues(params);
    return this;
  }



  public JsonParam(Class<T> paramType) {
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

  public JsonParam validate() {
    Objects.requireNonNull(jsonField,"Column shouldn't be null !");
    return this;
  }

  @Override
  public String toString() {
    return "QueryParam{" +
      "paramType=" + paramType +
      ", column='" + jsonField + '\'' +
      ", params=" + params +
      '}';
  }
}
