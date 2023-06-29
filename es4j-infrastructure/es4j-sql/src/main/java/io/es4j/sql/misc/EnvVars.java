/*******************************************************************************
 * ******************************************************************************
 *  * *
 *  *  * @author <a href="mailto:juglair.b@gmail.com">Benato J.</a>
 *  *
 *  *****************************************************************************
 ******************************************************************************/

package io.es4j.sql.misc;

public class EnvVars {
    private EnvVars() {
    }

    public static final String LIQUIBASE_TAG = System.getenv().getOrDefault("LIQUIBASE_TAG", "");
    public static final Boolean LIQUIBASE = Boolean.valueOf(System.getenv().getOrDefault("LIQUIBASE", "false"));
    public static final Boolean TCP_CORK = Boolean.valueOf(System.getenv().getOrDefault("TCP_CORK", "true"));
    public static final Boolean USE_ALPN = Boolean.valueOf(System.getenv().getOrDefault("USE_ALPN", "true"));
    public static final Boolean TCP_FAST_OPEN = Boolean.valueOf(System.getenv().getOrDefault("TCP_FAST_OPEN", "true"));
    public static final Boolean TCP_REUSE_PORT = Boolean.valueOf(System.getenv().getOrDefault("REUSE_PORT", "true"));
    public static final Boolean TCP_KEEP_ALIVE = Boolean.valueOf(System.getenv().getOrDefault("TCP_KEEP_ALIVE", "false"));
    public static final Boolean TCP_QUICK_ACK = Boolean.valueOf(System.getenv().getOrDefault("TCP_QUICK_ACK", "true"));
    public static final Boolean TCP_NO_DELAY = Boolean.valueOf(System.getenv().getOrDefault("TCP_NO_DELAY", "true"));
    public static final Boolean LOG_ACTIVITY = Boolean.valueOf(System.getenv().getOrDefault("LOG_ACTIVITY", "false"));
    public static final Integer WEB_CLIENT_POOL_SIZE = Integer.parseInt(System.getenv().getOrDefault("WEB_CLIENT_POOL_SIZE", "10"));
    public static final String PG_HOST = System.getenv().getOrDefault("PG_HOST", "127.0.0.1");
    public static final String PG_DATABASE = System.getenv().getOrDefault("PG_DATABASE", "postgres");
    public static final String PG_USER = System.getenv().getOrDefault("PG_USER", "postgres");
    public static final String PG_PASSWORD = System.getenv().getOrDefault("PG_PASSWORD", "postgres");
    public static final String SSL_MODE = System.getenv().getOrDefault("SSL_MODE", "ALLOW");
    public static final Integer PG_PORT = Integer.parseInt(System.getenv().getOrDefault("PG_PORT", "5432"));
    public static final String SCHEMA = System.getenv().getOrDefault("SCHEMA", "es4j");
    public static final String CHANGELOG = System.getenv().getOrDefault("CHANGELOG", "changelog.xml");
    public static final int REPOSITORY_RETRY_BACKOFF = Integer.parseInt(System.getenv().getOrDefault("REPOSITORY_RETRY_BACKOFF", "10"));
    public static final int REPOSITORY_MAX_RETRY = Integer.parseInt(System.getenv().getOrDefault("REPOSITORY_MAX_RETRY", "3"));
    public static final int REPOSITORY_STREAM_BATCH_SIZE = Integer.parseInt(System.getenv().getOrDefault("REPOSITORY_STREAM_BATCH_SIZE", "25"));
    public static final Integer PG_RECONNECT = Integer.parseInt(System.getenv().getOrDefault("PG_RECONNECT", "5"));
    public static final Integer PG_CONNECT_INTERVAL = Integer.parseInt(System.getenv().getOrDefault("PG_CONNECT_INTERVAL", "5"));
    public static final Integer PG_CONNECTION_TIMEOUT = Integer.parseInt(System.getenv().getOrDefault("PG_CONNECTION_TIMEOUT", "5"));
    public static final Integer PG_CLEANER_PERIOD = Integer.parseInt(System.getenv().getOrDefault("PG_CLEANER_PERIOD", "1000"));
}
