package io.vertx.eventx.solr;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.common.exceptions.EventxException;

public class SolrClientException extends EventxException {
  public SolrClientException(EventXError eventxError) {
    super(eventxError);
  }

  public SolrClientException(Throwable throwable) {
    super(throwable);
  }

  public SolrClientException(EventXError eventxError, Throwable throwable) {
    super(eventxError, throwable);
  }

  public SolrClientException(String cause, String hint, Integer errorCode) {
    super(cause, hint, errorCode);
  }

  public SolrClientException(String cause, String hint, Integer errorCode, Throwable throwable) {
    super(cause, hint, errorCode, throwable);
  }
}
