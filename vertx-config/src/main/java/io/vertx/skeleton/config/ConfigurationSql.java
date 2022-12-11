package io.vertx.skeleton.config;

public class ConfigurationSql {

  public static String parse(String sql, Object... arguments) {
    return java.text.MessageFormat.format(sql, arguments);
  }

  public static final String function = "CREATE OR REPLACE FUNCTION %s.configuration_publisher() " +
      " RETURNS trigger AS " +
      " $$ " +
      " BEGIN " +
      "    PERFORM pg_notify('configuration_channel', concat(NEW.name::text,'::', NEW.class::text, '::', NEW.tenant::text)); " +
      "    RETURN NEW; " +
      " END; " +
      " $$ LANGUAGE plpgsql; ";
  public static final String trigger = """
    CREATE OR REPLACE TRIGGER configuration_trigger AFTER INSERT OR UPDATE OR DELETE ON {0}.configuration FOR EACH ROW EXECUTE PROCEDURE configuration_publisher();
    """;

  public static final String triggerPg13 = """
    CREATE TRIGGER configuration_trigger AFTER INSERT OR UPDATE OR DELETE ON {0}.configuration FOR EACH ROW EXECUTE PROCEDURE configuration_publisher();
    """;
  public static final String sequence = "create sequence if not exists {0}.configuration_seq;";
  public static final String table = """
    create table if not exists {0}.configuration (
        name          text,
        class         text,
        data          jsonb,
        id            bigint    default nextval({1}) not null primary key,
        tenant        text,
        creation_date timestamp default now(),
        last_update   timestamp default now(),
        version       integer   default 0,
        unique (name, class, tenant)
    );
    """;
  public static final String index = "create index if not exists configuration_index on {0}.configuration (name, class, tenant);";

}
