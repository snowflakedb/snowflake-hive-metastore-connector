/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.hive.listener;

import com.snowflake.jdbc.client.SnowflakeClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
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

  public SnowflakeHiveListener(Configuration config)
  {
    super(config);
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
      SnowflakeClient.createAndExecuteEventForSnowflake(tableEvent);
    }
  }
}
