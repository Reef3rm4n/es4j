package io.vertx.eventx.http;

import io.vertx.eventx.common.EventXError;

import java.util.List;

public interface RouteErrorMapper {


  EventXError map(Throwable throwable);

  List<Class<Throwable>> knownThrowables();

  String tenantID();
}
