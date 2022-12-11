package io.vertx.skeleton.models;


public class EmptyQuery implements Query {
  @Override
  public QueryOptions options() {
    throw new IllegalArgumentException();
  }

}
