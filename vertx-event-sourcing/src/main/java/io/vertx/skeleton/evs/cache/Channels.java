package io.vertx.skeleton.evs.cache;

public class AddressResolver {

  private AddressResolver() {
  }


  public static <T> String actorChannel(Class<T> tClass) {
    return "post.available.actor." + tClass.getName();
  }

  public static <T> String getAvailableActors(Class<T> tClass) {
    return "get.available.actors." + tClass.getName();
  }

  public static <T> String synchronizerService(Class<T> tClass) {
    return tClass.getName() + ".synchronizer";
  }

}
