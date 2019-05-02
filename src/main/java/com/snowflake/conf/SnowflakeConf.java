/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.conf;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The configuration for the Snowflake Hive metastore listener,
 * which is stored in snowflake-config.xml
 * @author xma
 */
public class SnowflakeConf extends Configuration
{
  private static final Logger log = LoggerFactory.getLogger(
      SnowflakeConf.class);

  private static final String NOT_A_SF_JDBC_PROPERTY = null;

  public enum ConfVars
  {
    SNOWFLAKE_JDBC_USERNAME("snowflake.jdbc.username", "user",
      "The user to use to connect to Snowflake."),
    SNOWFLAKE_JDBC_PASSWORD("snowflake.jdbc.password", "password",
      "The password to use to connect to Snowflake."),
    SNOWFLAKE_JDBC_ACCOUNT("snowflake.jdbc.account", "account",
      "The account to use to connect to Snowflake."),
    SNOWFLAKE_JDBC_ROLE("snowflake.jdbc.role", "role",
      "The role to use to connect to Snowflake."),
    SNOWFLAKE_JDBC_DB("snowflake.jdbc.db", "db",
      "The database to use to connect to Snowflake."),
    SNOWFLAKE_JDBC_SCHEMA("snowflake.jdbc.schema", "schema",
      "The schema to use to connect to Snowflake."),
    SNOWFLAKE_JDBC_SSL("snowflake.jdbc.ssl", "ssl",
      "Use ssl to connect to Snowflake"),
    SNOWFLAKE_JDBC_CONNECTION("snowflake.jdbc.connection", "connection",
      "The Snowflake connection string."),
    SNOWFLAKE_STAGE_FOR_HIVE_EXTERNAL_TABLES(
        "snowflake.hive-metastore-listener.stage", NOT_A_SF_JDBC_PROPERTY,
        "The stage to use when creating external tables with Snowflake"),
    SNOWFLAKE_INTEGRATION_FOR_HIVE_EXTERNAL_TABLES(
        "snowflake.hive-metastore-listener.integration", NOT_A_SF_JDBC_PROPERTY,
        "The storage integration to use when creating external tables with " +
            "Snowflake"),
    SNOWFLAKE_HIVEMETASTORELISTENER_RETRY_COUNT(
        "snowflake.hive-metastore-listener.retry.count", NOT_A_SF_JDBC_PROPERTY,
        "The number of retries when connecting with Snowflake"),
    SNOWFLAKE_HIVEMETASTORELISTENER_RETRY_TIMEOUT_MILLISECONDS(
        "snowflake.hive-metastore-listener.retry.timeout",
        "retryTimeout",
        "The time between retries when connecting with Snowflake, in milliseconds"),
    SNOWFLAKE_CLIENT_FORCE_SYNCHRONOUS(
        "snowflake.hive-metastore-listener.force-synchronous",
        NOT_A_SF_JDBC_PROPERTY,
        "Forces the Hive listener to wait for Snowflake queries to execute " +
            "instead of queueing them for a background task."),
    SNOWFLAKE_CLIENT_THREAD_COUNT(
        "snowflake.hive-metastore-listener.client-thread-count",
        NOT_A_SF_JDBC_PROPERTY,
        "Determines the number of possible concurrent clients to communicate " +
            "with Snowflake"),
    SNOWFLAKE_ENABLE_CREDENTIALS_FROM_HIVE_CONF(
      "snowflake.hive-metastore-listener.enable-creds-from-conf",
      NOT_A_SF_JDBC_PROPERTY,
      "Whether the Hive metastore listener should read credentials from Hive " +
          "configurations."),
    SNOWFLAKE_TABLE_FILTER_REGEX(
        "snowflake.hive-metastore-listener.table-filter-regex",
        NOT_A_SF_JDBC_PROPERTY,
        "A regex to filter events with. Tables with names that match " +
            "this regex will be be ignored."),
    SNOWFLAKE_DATABASE_FILTER_REGEX(
        "snowflake.hive-metastore-listener.database-filter-regex",
        NOT_A_SF_JDBC_PROPERTY,
        "A regex to filter events with. Databases with names that match " +
            "this regex will be be ignored."),
    SNOWFLAKE_DATA_COLUMN_CASING(
        "snowflake.hive-metastore-listener.data-column-casing",
        NOT_A_SF_JDBC_PROPERTY,
        "Specifies the casing for columns in the data. Acceptable values are " +
            "UPPER, LOWER"
    );

    public static final Map<String, ConfVars> BY_VARNAME =
        Arrays.stream(ConfVars.values())
            .collect(Collectors.toMap(e -> e.varname, e -> e));

    public static ConfVars findByName(String varname)
    {
      return BY_VARNAME.get(varname);
    }

    ConfVars(String varname, String snowflakePropertyName,
             String description)
    {
      this.varname = varname;
      this.snowflakePropertyName = snowflakePropertyName;
      this.description = description;
    }

    public String getVarname()
    {
      return this.varname;
    }

    public String getSnowflakePropertyName()
    {
      return this.snowflakePropertyName;
    }

    private final String varname;
    private final String snowflakePropertyName;
    private final String description;
  }

  private static URL snowflakeConfigUrl;

  static
  {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null)
    {
      classLoader = SnowflakeConf.class.getClassLoader();
    }

    snowflakeConfigUrl = findConfigFile(classLoader, "snowflake-config.xml");
  }

  private static URL findConfigFile(ClassLoader classLoader, String name)
  {
    URL result = classLoader.getResource(name);
    if(result == null)
    {
      log.error("Could not find the Snowflake configuration file. " +
          "Ensure that it's named snowflake-config.xml and " +
          "it is in the hive classpath");
    }
    return result;
  }

  private void initialize()
  {
    if (snowflakeConfigUrl != null)
    {
      addResource(snowflakeConfigUrl);
    }
  }

  public SnowflakeConf()
  {
    super(false);
    initialize();
  }

}
