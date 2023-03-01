/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.eventx.common;

public class EnvVars {

  private EnvVars() {
  }

  public static final int REPOSITORY_RETRY_BACKOFF = Integer.parseInt(System.getenv().getOrDefault("REPOSITORY_RETRY_BACKOFF", "10"));
  public static final int REPOSITORY_MAX_RETRY = Integer.parseInt(System.getenv().getOrDefault("REPOSITORY_MAX_RETRY", "3"));
  public static final int REPOSITORY_STREAM_BATCH_SIZE = Integer.parseInt(System.getenv().getOrDefault("REPOSITORY_STREAM_BATCH_SIZE", "25"));
  public static final int CACHE_RELOAD_INTERVAL = Integer.parseInt(System.getenv().getOrDefault("CACHE_REFRESH_INTERVAL", "60"));

}
