/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.hive.listener;

import com.snowflake.conf.SnowflakeJdbcConf;
import com.snowflake.jdbc.client.SnowflakeClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The listener for Hive
 * This listener will get notified when the corresponding command is called
 */
public class SnowflakeHiveListener extends MetaStoreEventListener
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeHiveListener.class);

  private static SnowflakeJdbcConf snowflakeJdbcConf;

  public SnowflakeHiveListener(Configuration config)
  {
    super(config);
    // generate the snowflake jdbc conf
    snowflakeJdbcConf = new SnowflakeJdbcConf();
    log.info("SnowflakeHiveListener created");
  }

  /**
   * The listener for the create table command
   * @param tableEvent
   */
  @Override
  public void onCreateTable(CreateTableEvent tableEvent)
  {
    log.info("SnowflakeHiveListener: CreateTableEvent received");
    if (tableEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(tableEvent,
          snowflakeJdbcConf);
    }
  }

  /**
   * The listener for the drop table command
   * @param tableEvent
   */
  @Override
  public void onDropTable(DropTableEvent tableEvent)
  {
    log.info("SnowflakeHiveListener: DropTableEvent received");
    if (tableEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(tableEvent,
          snowflakeJdbcConf);
    }
  }
}
