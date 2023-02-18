/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.skeleton.solr;

import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.RepositoryRecord;
import io.vertx.skeleton.models.Query;
import io.vertx.skeleton.models.exceptions.SolrException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.shareddata.Lock;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CursorMarkParams;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;


//todo find a non blocking version of solr client, for now lets just offload to worker threads.
public record SolrCollection<K, V extends RepositoryRecord<V>, Q extends Query, S, Y extends Query>(
  SolrClient solrClient,
  CollectionMapper<K, V, Q, S, Y> solrMapper,
  Vertx vertx,
  JsonObject configuration
) implements Collection<K, V, Q, Y> {

  private static final Logger logger = LoggerFactory.getLogger(SolrCollection.class);

  @Override
  public Uni<V> getById(final String id) {
    return vertx.executeBlocking(Uni.createFrom().item(
          Unchecked.supplier(() -> {
              try {
                return solrClient.getById(solrMapper.collection(), id);
              } catch (SolrServerException e) {
                logger.error("Solr server exception in  " + solrMapper.collection(), e);
                throw new SolrException(new Error("Solr Collection error " + solrMapper.collection(), e.getMessage(), 500));
              } catch (IOException e) {
                logger.error("IoException, in solr  " + solrMapper.collection(), e);
                throw new SolrException(new Error("Solr repository error for " + solrMapper.collection(), e.getMessage(), 500));
              }
            }
          )
        )
        .map(Unchecked.function(solrDocument -> {
              if (solrDocument._size() > 1) {
                throw new SolrException(new Error("Key Query Wrong", "Key query is returning more than one object from solr solrMapper.collection()", 400));
              }
              return solrMapper.fromDocumentToRecord(solrDocument);
            }
          )
        )
    );
  }

  @Override
  public Uni<V> get(K key) {
    return vertx.executeBlocking(Uni.createFrom().item(
          Unchecked.supplier(() -> {
              try {
                return solrClient.query(solrMapper.collection(), solrMapper.key(key));
              } catch (SolrServerException e) {
                logger.error("Solr server exception in  " + solrMapper.collection(), e);
                throw new SolrException(new Error("Solr Collection error " + solrMapper.collection(), e.getMessage(), 500));
              } catch (IOException e) {
                logger.error("IoException, in solr  " + solrMapper.collection(), e);
                throw new SolrException(new Error("Solr repository error for  " + solrMapper.collection(), e.getMessage(), 500));
              }
            }
          )
        )
      )
      .map(Unchecked.function(solrDocument -> {
            if (solrDocument._size() > 1) {
              throw new SolrException(new Error("Key Query Wrong", "Document not found in collection: " + solrMapper.collection(), 400));
            }
            final var solrBean = solrDocument.getBeans(solrMapper.solrObjectClass()).stream()
              .findFirst()
              .orElseThrow(() -> new SolrException(new Error("Solr Document Not found", "Document not found", 40)));
            return JsonObject.mapFrom(solrBean).mapTo(solrMapper.valueClass());
          }
        )
      );
  }

  @Override
  public Uni<Void> addAndCommit(V value) {
    return vertx.executeBlocking(
        Uni.createFrom().item(
          Unchecked.supplier(() -> {
              try {
                final var response = solrClient.addBean(solrMapper.collection(), solrMapper.mapToSolrBean(value));
                solrClient.commit(solrMapper.collection());
                logger.debug("Added to solr collection :" + response);
                return response;
              } catch (SolrServerException e) {
                logger.error("Solr server exception", e);
                throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
              } catch (Exception e) {
                logger.error("Exception, error within the callback", e);
                throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
              }
            }
          )
        )
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> add(V value) {
    return vertx.executeBlocking(
      Uni.createFrom().item(
        Unchecked.supplier(() -> {
            try {
              return solrClient.addBean(solrMapper.collection(), solrMapper.mapToSolrBean(value));
            } catch (SolrServerException e) {
              logger.error("Solr server exception", e);
              throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
            } catch (Exception e) {
              logger.error("IoException, error within the callback", e);
              throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
            }
          }
        )
      )
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> addBatch(List<V> values) {
    return vertx.executeBlocking(
      Uni.createFrom().item(
        Unchecked.supplier(() -> {
            try {
              return solrClient.addBeans(solrMapper.collection(), values.stream().map(solrMapper::mapToSolrBean).toList());
            } catch (SolrServerException e) {
              logger.error("Solr server exception", e);
              throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
            } catch (Exception e) {
              logger.error("IoException, error within the callback", e);
              throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
            }
          }
        )
      )
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> addBatchAndCommit(List<V> values) {
    return vertx.executeBlocking(
      Uni.createFrom().item(
        Unchecked.supplier(() -> {
            try {
              solrClient.addBeans(solrMapper.collection(), values.stream().map(solrMapper::mapToSolrBean).toList());
              return solrClient.commit(solrMapper.collection());
            } catch (SolrServerException e) {
              logger.error("Solr server exception", e);
              throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
            } catch (Exception e) {
              logger.error("IoException, error within the callback", e);
              throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
            }
          }
        )
      )
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> commit() {
    return vertx.executeBlocking(
      Uni.createFrom().item(
        Unchecked.supplier(() -> {
            try {
              return solrClient.commit(solrMapper.collection());
            } catch (SolrServerException e) {
              logger.error("Solr server exception", e);
              throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
            } catch (Exception e) {
              logger.error("IoException, error within the callback", e);
              throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
            }
          }
        )
      )
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> rollback() {
    return vertx.executeBlocking(
      Uni.createFrom().item(
        Unchecked.supplier(() -> {
            try {
              return solrClient.rollback(solrMapper.collection());
            } catch (SolrServerException e) {
              logger.error("Solr server exception", e);
              throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
            } catch (Exception e) {
              logger.error("IoException, error within the callback", e);
              throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
            }
          }
        )
      )
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> delete(String id) {
    return vertx.executeBlocking(
      Uni.createFrom().item(
        Unchecked.supplier(() -> {
            try {
              return solrClient.deleteById(solrMapper.collection(), id);
            } catch (SolrServerException e) {
              logger.error("Solr server exception", e);
              throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
            } catch (Exception e) {
              logger.error("IoException, error within the callback", e);
              throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
            }
          }
        )
      )
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteAndCommit(String id) {
    return vertx.executeBlocking(
      Uni.createFrom().item(
        Unchecked.supplier(() -> {
            try {
              solrClient.deleteById(solrMapper.collection(), id);
              return solrClient.commit(solrMapper.collection());
            } catch (SolrServerException e) {
              logger.error("Solr server exception", e);
              throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
            } catch (Exception e) {
              logger.error("IoException, error within the callback", e);
              throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
            }
          }
        )
      )
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteBatch(List<String> ids) {
    return vertx.executeBlocking(
      Uni.createFrom().item(
        Unchecked.supplier(() -> ids.stream().map(
            id -> {
              try {
                return solrClient.deleteById(solrMapper.collection(), id);
              } catch (SolrServerException e) {
                logger.error("Solr server exception", e);
                throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
              } catch (Exception e) {
                logger.error("IoException, error within the callback", e);
                throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
              }
            }
          ).toList()
        )
      )
    ).replaceWithVoid();
  }

  @Override
  public Uni<Void> deleteBatchAndCommit(List<String> ids) {
    return vertx.executeBlocking(
      Uni.createFrom().item(
        Unchecked.supplier(() -> ids.stream().map(
            id -> {
              try {
                solrClient.deleteById(solrMapper.collection(), id);
                return solrClient.commit(solrMapper.collection());
              } catch (SolrServerException e) {
                logger.error("Solr server exception", e);
                throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
              } catch (Exception e) {
                logger.error("IoException, error within the callback", e);
                throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
              }
            }
          ).toList()
        )
      )
    ).replaceWithVoid();
  }

  @Override
  public Uni<List<V>> query(Q query) {
    return vertx.executeBlocking(Uni.createFrom().item(
        Unchecked.supplier(() -> {
            try {
              final var solrQuery = solrMapper.query(query);
              solrMapper.filterOptions(solrQuery, query.options());
              final var queryResult = solrClient.query(solrMapper.collection(), solrQuery);
              if (queryResult.getResults().isEmpty()) {
                throw new SolrException(new Error("Not Found", "Empty query result", 400));
              }
              return queryResult.getBeans(solrMapper.solrObjectClass()).stream().map(solrMapper::fromBeanToRecord).toList();
            } catch (Exception exception) {
              if (exception instanceof SolrServerException notifierException) {
                throw notifierException;
              }
              if (exception instanceof org.apache.solr.common.SolrException exception1) {
                throw exception1;
              }
              if (exception instanceof SolrException solrException) {
                throw solrException;
              }
              throw new SolrException(new Error(exception.getMessage(), exception.getLocalizedMessage(), 500));
            }
          }
        )
      )
    );
  }

  @Override
  public Uni<Void> streamProcess(Q query, Function<V, Uni<Void>> function) {
    return vertx.executeBlocking(
        Uni.createFrom().item(
          Unchecked.supplier(
            () -> {
              try {
                final var solrQuery = solrMapper.query(query);
                return solrClient.queryAndStreamResponse(
                  solrMapper.collection(),
                  solrQuery,
                  new StreamingResponseCallback() {
                    @Override
                    public void streamSolrDocument(SolrDocument doc) {
                      final var bean = solrMapper.fromDocumentToRecord(doc);
                      function.apply(bean)
                        .subscribe()
                        .with(
                          aVoid -> logger.info("Document processed " + doc.getFieldValue("uniqueKey")),
                          throwable -> logger.error("Error processing document", throwable)
                        );
                    }

                    @Override
                    public void streamDocListInfo(long numFound, long start, Float maxScore) {
                      logger.info("Stream found :" + numFound + ", starting at :" + start + ", query score :" + maxScore);
                    }
                  }
                );
              } catch (SolrServerException e) {
                logger.error("Solr server exception, stream interrupted", e);
                throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
              } catch (Exception e) {
                logger.error("IoException, error within the callback", e);
                throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
              }
            }
          )
        )
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Void> cursorStreamProcess(Consumer<V> handler, Q query, Lock distributedLock) {
    final var cursorMark = new AtomicReference<>(CursorMarkParams.CURSOR_MARK_START);
    return Multi.createBy().repeating().uni(() -> cursorQuery(query, cursorMark.get()))
      .until(tuple3 -> {
          // changes the last cursor mark in the tuple
          cursorMark.set(tuple3.getItem2());
          // checks internal cursor state for EOF, if not Multi will repeat the uni call to cursor query
          return tuple3.getItem3();
        }
      ).onItem().invoke(tuple3 -> tuple3.getItem1().forEach(handler))
      .onItem().ignoreAsUni()
      .onItemOrFailure()
      .invoke((item, throwable) -> {
          distributedLock.release();
          if (throwable != null) {
            logger.error("Error during cursor stream", throwable);
          } else {
            logger.info("Cursor processing finished, lock released");
          }
        }
      )
      .replaceWithVoid();
  }

  @Override
  public Uni<Tuple3<List<V>, String, Boolean>> cursorQuery(Q query, String cursorMark) {
    return vertx.executeBlocking(Uni.createFrom().item(
        Unchecked.supplier(() -> {
            try {
              final var solrQuery = solrMapper.query(query);
              solrQuery.addSort("id", SolrQuery.ORDER.asc);
              solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark == null ? CursorMarkParams.CURSOR_MARK_START : cursorMark);
              final var queryResult = solrClient.query(solrMapper.collection(), solrQuery);
              if (queryResult.getResults().isEmpty()) {
                throw new SolrException(new Error("Not Found", "Empty query result", 400));
              }
              final var resultList = queryResult.getBeans(solrMapper.solrObjectClass()).stream().map(solrMapper::fromBeanToRecord).toList();
              final var nextCursorMark = queryResult.getNextCursorMark();
              return Tuple3.of(resultList, nextCursorMark, nextCursorMark.equals(cursorMark));
            } catch (SolrServerException e) {
              logger.error("Solr server exception, stream interrupted", e);
              throw new SolrException(new Error("Solr Stream interrupted", e.getMessage(), 500));
            } catch (Exception e) {
              logger.error("IoException, error within the callback", e);
              throw new SolrException(new Error("Callback return error", e.getMessage(), 500));
            }
          }
        )
      )
    );
  }

  @Override
  public Uni<List<V>> search(final Y search) {
    return vertx.executeBlocking(Uni.createFrom().item(
        Unchecked.supplier(() -> {
            try {
              final var solrQuery = solrMapper.search(search);
              solrMapper.filterOptions(solrQuery, search.options());
              final var queryResult = solrClient.query(solrQuery);
              if (queryResult.getResults().isEmpty()) {
                throw new SolrException(new Error("Not Found", "Empty query result", 400));
              }
              return queryResult.getBeans(solrMapper.solrObjectClass()).stream().map(solrMapper::fromBeanToRecord).toList();
            } catch (Exception exception) {
              throw new SolrException(new Error(exception.getMessage(), exception.getLocalizedMessage(), 500));
            }
          }
        )
      )
    );
  }
}
