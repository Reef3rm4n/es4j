package io.vertx.skeleton.sql.models;

public class QueryParam<T> {

  private final Class<T> paramType;
  private String column;
  private T param;

  public String column() {
    return column;
  }

  public QueryParam<T> setColumn(String column) {
    this.column = column;
    return this;
  }

  public T param() {
    return param;
  }

  public QueryParam<T> setParam(T param) {
    this.param = param;
    return this;
  }




  public QueryParam(Class<T> paramType) {
    this.paramType = paramType;
  }

  @Override
  public String toString() {
    return "QueryParam{" +
      "paramType=" + paramType +
      ", column='" + column + '\'' +
      ", params=" + param +
      '}';
  }
}
