/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.hive.listener;

import com.snowflake.conf.SnowflakeConf;
import com.snowflake.jdbc.client.SnowflakeClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
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

  private static SnowflakeConf snowflakeConf;

  public SnowflakeHiveListener(Configuration config)
  {
    super(config);
    // generate the snowflake jdbc conf
    snowflakeConf = new SnowflakeConf();
    log.info("SnowflakeHiveListener created");
  }

  /**
   * The listener for the create table command
   * @param tableEvent An event that was listened for
   */
  @Override
  public void onCreateTable(CreateTableEvent tableEvent)
  {
    log.info("SnowflakeHiveListener: CreateTableEvent received");
    if (tableEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(tableEvent,
                                                        snowflakeConf);
    }
  }

  /**
   * The listener for the drop table command
   * @param tableEvent An event that was listened for
   */
  @Override
  public void onDropTable(DropTableEvent tableEvent)
  {
    log.info("SnowflakeHiveListener: DropTableEvent received");
    if (tableEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(tableEvent,
                                                        snowflakeConf);
    }
  }

  /**
   * The listener for the add partition command
   * @param partitionEvent An event that was listened for
   */
  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent)
  {
    log.info("SnowflakeHiveListener: AddPartitionEvent received");
    if (partitionEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(partitionEvent,
                                                        snowflakeConf);
    }
  }

  /**
   * The listener for the drop partition command
   * @param partitionEvent An event that was listened for
   */
  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent)
  {
    log.info("SnowflakeHiveListener: DropPartitionEvent received");
    if (partitionEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(partitionEvent,
                                                        snowflakeConf);
    }
  }
}
