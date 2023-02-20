package io.vertx.skeleton.sql.models;

import java.util.*;

public class JsonQueryParam<T> {

  private final Class<T> paramType;
  private String column;
  private Queue<String> jsonFields;
  private T param;

  public String column() {
    return column;
  }

  public JsonQueryParam<T> setColumn(String column) {
    this.column = column;
    return this;
  }

  public T param() {
    return param;
  }

  public JsonQueryParam<T> setParam(T param) {
    this.param = param;
    return this;
  }



  public JsonQueryParam(Class<T> paramType) {
    this.paramType = paramType;
  }

  public JsonQueryParam validate() {
    Objects.requireNonNull(column,"Column shouldn't be null !");
    return this;
  }

  @Override
  public String toString() {
    return "QueryParam{" +
      "paramType=" + paramType +
      ", column='" + column + '\'' +
      ", params=" + param +
      ", jsonParams=" + jsonFields +
      '}';
  }

  public Queue<String> jsonFields() {
    return jsonFields;
  }

  public JsonQueryParam<T> addJsonFields(String jsonField) {
    if (jsonFields == null) {
      this.jsonFields = new ArrayDeque<>();
    }
    jsonFields.add(jsonField);
    return this;
  }

}
