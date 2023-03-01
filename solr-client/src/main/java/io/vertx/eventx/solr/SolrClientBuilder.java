package io.vertx.eventx.solr;

import io.vertx.core.json.JsonObject;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

import java.util.List;
import java.util.Optional;

public class SolrClientBuilder {

  private SolrClientBuilder(){}
  public static final Boolean KUBERNETES = Boolean.parseBoolean(System.getenv().getOrDefault("KUBERNETES", "false"));
  public static SolrClient setUpSolrClient(JsonObject config) {
    if (config.getString("baseSolrUrl") != null) {
      if (KUBERNETES) {
        final var http2ClientBuilder = new Http2SolrClient.Builder();
        if (config.getString("solrUsername") != null) {
          http2ClientBuilder.withBasicAuthCredentials(config.getString("solrUserName"), config.getString("solrPassword"));
        }
        return new CloudHttp2SolrClient
          .Builder(List.of("command-zookeeper"), Optional.of(""))
          .withInternalClientBuilder(http2ClientBuilder)
          .withParallelUpdates(config.getBoolean("solrParallelUpdated", false))
          .build();
      } else {
        final var http2ClientBuilder = new Http2SolrClient.Builder(config.getString("baseSolrUrl"));
        if (config.getString("solrUsername") != null) {
          http2ClientBuilder.withBasicAuthCredentials(config.getString("solrUserName"), config.getString("solrPassword"));
        }
        return http2ClientBuilder.build();
      }
    }
    return null;
  }
}
