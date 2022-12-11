package io.vertx.skeleton.models;


public class SolrException extends VertxServiceException {

  public SolrException(Error error) {
    super(error);
  }

  public static SolrException illegalState() {
    return new SolrException(new Error("Illegal state", "", 500));
  }


  public static SolrException notImplemented() {
    return new SolrException(new Error("Not Implemented", "", 500));
  }


  public static SolrException notImplemented(String hint) {
    return new SolrException(new Error("Not Implemented", hint, 500));
  }


}
