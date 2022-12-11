package io.vertx.skeleton.evs.cache;


import java.util.Set;

public record HandlerList(
  Set<String> handlers
) {
}
