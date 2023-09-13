package io.es4j.infrastructure.cdc;

import org.postgresql.PGProperty;

import java.util.Properties;

public interface PostgresConfiguration {

    String DEFAULT_PORT = "5432";
    String MIN_SERVER_VERSION = "10.3";
    String DEFAULT_SSL_MODE = "disable";

    default String getPort() {
        return DEFAULT_PORT;
    }

    default String getMinServerVersion() {
        return MIN_SERVER_VERSION;
    }

    default String getReplication() {
        return "database";
    }

    default String getQueryMode() {
        return "simple";
    }

    default String getUrl() {
        return String.format("jdbc:postgresql://%s:%s/%s", getHost(), getPort(), getDatabase());
    }

    default String getPathToRootCert() {
        return null;
    }

    default String getSslPassword() {
        return null;
    }

    default String getPathToSslKey() {
        return null;
    }

    default String getPathToSslCert() {
        return null;
    }

    default String getSslMode() {
        return DEFAULT_SSL_MODE;
    }

    default Properties getReplicationProperties() {
        Properties properties = getQueryConnectionProperties();
        PGProperty.PREFER_QUERY_MODE.set(properties, getQueryMode());
        PGProperty.REPLICATION.set(properties, getReplication());
        return properties;
    }

    default Properties getQueryConnectionProperties() {
        Properties properties = new Properties();
        PGProperty.USER.set(properties, getUsername());
        PGProperty.PASSWORD.set(properties, getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, getMinServerVersion());
        PGProperty.SSL_MODE.set(properties, getSslMode());
        PGProperty.SSL_ROOT_CERT.set(properties, getPathToRootCert());
        PGProperty.SSL_CERT.set(properties, getPathToSslCert());
        PGProperty.SSL_PASSWORD.set(properties, getSslPassword());
        PGProperty.SSL_KEY.set(properties, getPathToSslKey());
        return properties;
    }

    String getHost();

    String getDatabase();

    String getUsername();

    String getPassword();

}
