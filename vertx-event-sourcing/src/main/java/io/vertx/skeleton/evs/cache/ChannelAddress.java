package io.vertx.skeleton.evs.cache;

public class ChannelAddress {

  private ChannelAddress() {
  }


  public static <T> String broadcastChannel(Class<T> tClass) {
    return "post.available.actor." + tClass.getName();
  }

  public static <T> String invokeChannel(Class<T> tClass) {
    return "invoke.actors." + tClass.getName();
  }

}
