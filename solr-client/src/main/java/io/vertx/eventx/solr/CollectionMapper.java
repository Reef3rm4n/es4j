/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.eventx.solr;

import io.vertx.eventx.common.EventXError;
import io.vertx.eventx.sql.models.Query;
import io.vertx.eventx.sql.models.QueryOptions;
import io.vertx.eventx.sql.models.RepositoryRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public interface CollectionMapper<K, V extends RepositoryRecord<V>, Q extends Query, S, Y extends Query> {


  SolrQuery key(K key);

  Class<V> valueClass();

  Class<K> keyClass();

  SolrQuery query(Q queryFilter);

  default SolrQuery search(Y searchFilter) {
    throw new SolrClientException(new EventXError("search Not implemented", "search Not implemented", 999));
  }

  Class<S> solrObjectClass();

  S mapToSolrBean(V value);

  V fromDocumentToRecord(SolrDocument value);

  V fromBeanToRecord(S value);

  String collection();

  default void addFilter(SolrQuery query, String fieldName, List<? extends Enum> fieldValues) {
    if (fieldValues != null && !fieldValues.isEmpty()) {
      fieldValues.forEach(fieldValue -> query.addFilterQuery(fieldName + ":" + fieldValue.name()));
    }
  }

  default void addFilter(List<String> fieldValues, SolrQuery solrQuery, String fieldName) {
    if (fieldValues != null && !fieldValues.isEmpty()) {
      fieldValues.forEach(fieldValue -> solrQuery.addFilterQuery(fieldName + ":" + fieldValue));
    }
  }

  default void addFilter(String fieldValue, SolrQuery solrQuery, String fieldName) {
    if (fieldValue != null) {
      solrQuery.addFilterQuery(fieldName + ":" + fieldValue);
    }
  }

  default void filterOptions(SolrQuery solrQuery, QueryOptions options) {
    if (options != null) {
      if (options.tenantId() != null) {
        solrQuery.addFilterQuery("tenandId:" + "\"" + options.tenantId() + "\"");
      }
      if (options.pageSize() != null) {
        solrQuery.setRows(options.pageSize());
        if (options.pageNumber() != null) {
          solrQuery.setStart((options.pageNumber() * options.pageSize()));
        }
      }
      if (options.orderBy() != null) {
        if (Boolean.TRUE.equals(options.desc())) {
          solrQuery.addSort(snakeToCamel(options.orderBy()), SolrQuery.ORDER.desc);
        }
        solrQuery.addSort(snakeToCamel(options.orderBy()), SolrQuery.ORDER.asc);
      }
      addFromToFilter(options.creationDateFrom(), options.creationDateTo(), "creationDate", solrQuery);
      addFromToFilter(options.lastUpdateFrom(), options.lastUpdateTo(), "lastUpdate", solrQuery);
    }
  }

  public static String snakeToCamel(String str) {
    // Capitalize first letter of string
    str = str.toLowerCase();
    // Run a loop till string
    // string contains underscore
    while (str.contains("_")) {
      // Replace the first occurrence
      // of letter that present after
      // the underscore, to capitalize
      // form of next letter of underscore
      str = str
        .replaceFirst("_[a-z]",
          String.valueOf(Character.toUpperCase(str.charAt(str.indexOf("_") + 1)))
        );
    }
    return str;
  }

  default void addFromToFilter(Instant from, Instant to, String filterParam, SolrQuery solrQuery) {
    if (from != null && to != null) {
      solrQuery.addFilterQuery(filterParam + " : [" + from.truncatedTo(ChronoUnit.SECONDS) + " TO " + to.truncatedTo(ChronoUnit.SECONDS) + "]");
    } else if (from != null) {
      solrQuery.addFilterQuery(filterParam + " : [" + from.truncatedTo(ChronoUnit.SECONDS) + " TO * ]");
    } else if (to != null) {
      solrQuery.addFilterQuery(filterParam + " : [ * TO " + to.truncatedTo(ChronoUnit.SECONDS) + "]");
    }
  }
}
