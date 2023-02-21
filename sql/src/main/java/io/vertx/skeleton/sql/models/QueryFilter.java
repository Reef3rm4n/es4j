package io.vertx.skeleton.sql.models;

public class QueryFilter<T> {

  private final Class<T> paramType;
  private String column;
  private T param;

  public String column() {
    return column;
  }

  public QueryFilter<T> filterColumn(String column) {
    this.column = column;
    return this;
  }

  public T param() {
    return param;
  }

  public QueryFilter<T> filterParam(T param) {
    this.param = param;
    return this;
  }




  public QueryFilter(Class<T> paramType) {
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
