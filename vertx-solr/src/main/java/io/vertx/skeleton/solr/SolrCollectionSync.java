/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.skeleton.solr;


import io.vertx.skeleton.models.Error;
import io.vertx.skeleton.models.exceptions.GenericVerticleException;
import io.vertx.skeleton.orm.RecordRepository;
import io.vertx.skeleton.models.RepositoryRecord;
import io.vertx.skeleton.models.exceptions.GenericVerticleCommand;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;

public class SolrCollectionSync<K, V extends RepositoryRecord<V>> extends AbstractVerticle {
  private final Collection<K, V, ?, ?> solrCollection;
  private final RecordRepository<K, V, ?> repository;
  private static final Logger logger = LoggerFactory.getLogger(SolrCollectionSync.class);

  public SolrCollectionSync(
    Collection<K, V, ?, ?> solrCollection,
    RecordRepository<K, V, ?> repository
  ) {
    this.solrCollection = solrCollection;
    this.repository = repository;
  }

  // todo upon start this verticle should try to sync the collection with the current db state. this should be triggered from a rest endpoint
  // verticle should auto undeploy itself after the sync is done.
  // sync strategy must be of two types
  // 1) delta sync, this will be useful for historical data tables. This strategy will consist on a select that will retrieve the biggest id on the table
  // and compare it with greatest id on solr and start the delta from there.
  // 2) full sync, this will trigger a full sync of the collection useful for non historical data collections.
  // could for example make a massive select on db fetching all ID's on the table and cross check missing ones in solr
  // once that is done push the missing ones to the collection.

  @Override
  public Uni<Void> asyncStart() {
    logger.info("Starting " + this.getClass().getSimpleName() +  " " + this.deploymentID());
    return vertx.eventBus().consumer(repository.mapper().keyClass().getSimpleName())
      .handler(
        objectMessage -> {
          final var operation = GenericVerticleCommand.valueOf(objectMessage.headers().get("action"));
          final var value = JsonObject.mapFrom(objectMessage.body()).mapTo(solrCollection.solrMapper().valueClass());
          objectMessage.reply(202);
          Uni<Void> operationUni;
          if (operation == GenericVerticleCommand.PUT) {
            operationUni =  solrCollection.addAndCommit(value);
          } else if (operation == GenericVerticleCommand.REMOVE) {
            operationUni = solrCollection.delete(value.persistedRecord().id().toString());
          } else {
            throw new GenericVerticleException(new Error("Unsupported Operation", "Verticle received an unsupported operation for collection : " + solrCollection.solrMapper().collection(), 500));
          }
          operationUni
            .subscribe()
            .with(
              aVoid -> logger.info("Record updated in collection :" + solrCollection.solrMapper().collection())
              , throwable -> logger.error("Unable to update solr record for collection :" + solrCollection.solrMapper().collection(), throwable)
            );
        }
      )
      .exceptionHandler(this::handleException)
      .completionHandler();
  }

  private void handleException(Throwable throwable) {
    logger.error("Unhandled exception in solrCollection :" + solrCollection.solrMapper().collection(), throwable);
  }

  @Override
  public Uni<Void> asyncStop() {
    logger.info("Stopping " + this.getClass().getSimpleName() + this.deploymentID() + " -> " + config().encodePrettily());
    return Uni.createFrom().voidItem();
  }
}
