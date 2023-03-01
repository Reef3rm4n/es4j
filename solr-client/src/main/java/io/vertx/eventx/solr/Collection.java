/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.eventx.solr;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.eventx.sql.models.Query;
import io.vertx.eventx.sql.models.RepositoryRecord;
import io.vertx.mutiny.core.shareddata.Lock;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public interface Collection<K, V extends RepositoryRecord<V>, Q extends Query, S extends Query> {

  Uni<Void> addAndCommit(V value);

  Uni<V> get(K key);

  Uni<V> getById(String id);

  Uni<Void> add(V value);

  Uni<Void> addBatch(List<V> values);

  Uni<Void> addBatchAndCommit(List<V> values);

  Uni<Void> commit();

  Uni<Void> rollback();

  Uni<Void> delete(String id);

  Uni<Void> deleteAndCommit(String id);

  Uni<Void> deleteBatch(List<String> ids);

  CollectionMapper<K, V, Q, ?, S> solrMapper();

  Uni<Void> streamProcess(Q query, Function<V, Uni<Void>> function);

  Uni<Void> deleteBatchAndCommit(List<String> ids);

  Uni<List<V>> query(Q query);

  Uni<List<V>> search(S search);


  /**
   * Processes the cursor batch until it's completion
   *
   * @param query
   * @return Void signaling the end of the cursor iteration stream, or an exception signaling that the stream blew up.
   */
  Uni<Void> cursorStreamProcess(Consumer<V> handler, Q query, Lock distributedLock);


  /**
   * Fetches the next batch of results using the cursorMark as starting point
   *
   * @param query
   * @param cursorMark
   * @return list of results that can be processed, next cursor mark and a boolean that signals the end of the cursor iteration
   */
  Uni<Tuple3<List<V>, String, Boolean>> cursorQuery(Q query, String cursorMark);

}
