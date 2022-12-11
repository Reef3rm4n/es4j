/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.vertx.skeleton.orm;


public class Constants {
  public static final String TX_ID = "tx_id";
  public static final String ENTITY_ID = "entity_id";
  public static final String EVENT_VERSION = "event_version";
  public static final String EVENT = "event";
  public static final String COMMAND = "command";
  public static final String EVENT_TYPE = "event_type";
  public static final String TENANT = "tenant";
  public static final String STATE = "state";
  public static final String PAYLOAD = "payload";
  public static final String ERROR = "error";
  public static final String COMMAND_CLASS = "command_class";
  public static final String EVENT_CLASS = "event_class";
  public static final String VERTICLE_ID = "verticle_id";
  public static final String TIMER_ID = "timer_id";

  private Constants(){}

  public static final String PG_DATABASE = "pgDatabase";
  public static final String PG_PASSWORD = "pgPassword";
  public static final String PG_USER = "pgUser";
  public static final String PG_PORT = "pgPort";
  public static final String SCHEMA = "schema";
  public static final String CHANGELOG = "changelog";
  public static final String PG_HOST = "pgHost";
  public static final String JDBC_URL = "jdbcUrl";

}
