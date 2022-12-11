/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/


package io.vertx.skeleton.models;


import io.vertx.core.shareddata.Shareable;

public interface RepositoryRecord<V> extends Shareable {
  PersistedRecord persistedRecord();
  V with(PersistedRecord persistedRecord);
  default Boolean validate() {
    return true;
  }

}
