/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.hive.listener;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.jdbc.client.SnowflakeClient;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
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
    logTableEvent(tableEvent, tableEvent.getTable());
    if (tableEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(tableEvent,
                                                        snowflakeConf);
    }
    else
    {
      log.info("SnowflakeHiveListener: Nothing to do for CreateTableEvent");
    }
  }

  /**
   * The listener for the drop table command
   * @param tableEvent An event that was listened for
   */
  @Override
  public void onDropTable(DropTableEvent tableEvent)
  {
    logTableEvent(tableEvent, tableEvent.getTable());
    if (tableEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(tableEvent,
                                                        snowflakeConf);
    }
    else
    {
      log.info("SnowflakeHiveListener: Nothing to do for DropTableEvent");
    }
  }

  /**
   * The listener for the add partition command
   * @param partitionEvent An event that was listened for
   */
  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent)
  {
    logPartitionsEvent(partitionEvent, partitionEvent.getTable(),
                       partitionEvent.getPartitionIterator());
    if (partitionEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(partitionEvent,
                                                        snowflakeConf);
    }
    else
    {
      log.info("SnowflakeHiveListener: Nothing to do for AddPartitionEvent");
    }
  }

  /**
   * The listener for the drop partition command
   * @param partitionEvent An event that was listened for
   */
  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent)
  {
    logPartitionsEvent(partitionEvent, partitionEvent.getTable(),
                       partitionEvent.getPartitionIterator());
    if (partitionEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(partitionEvent,
                                                        snowflakeConf);
    }
    else
    {
      log.info("SnowflakeHiveListener: Nothing to do for DropPartitionEvent");
    }
  }

  /**
   * The listener for the alter table command
   * @param tableEvent An event that was listened for
   */
  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException
  {
    logTableEvent(tableEvent, tableEvent.getNewTable());
    if (tableEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(tableEvent,
                                                        snowflakeConf);
    }
    else
    {
      log.info("SnowflakeHiveListener: Nothing to do for DropPartitionEvent");
    }
  }

  /**
   * The listener for the alter partition command
   * @param partitionEvent An event that was listened for
   */
  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException
  {
    logPartitionsEvent(partitionEvent, partitionEvent.getTable(),
                       ImmutableList.<Partition>builder().add(
                           partitionEvent.getNewPartition()).build().iterator());
    if (partitionEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteEventForSnowflake(partitionEvent,
                                                        snowflakeConf);
    }
    else
    {
      log.info("SnowflakeHiveListener: Nothing to do for DropPartitionEvent");
    }
  }

  /**
   * Helper method for logging that an event occurred for a Hive table
   * @param event The event
   * @param hiveTable The Hive table associated with the event
   */
  private static void logTableEvent(ListenerEvent event, Table hiveTable)
  {
    Preconditions.checkNotNull(hiveTable);
    String tableName = Preconditions.checkNotNull(hiveTable.getTableName());
    log.info(String.format("SnowflakeHiveListener: %s received for table '%s'",
                           event.getClass().getSimpleName(),
                           tableName));
  }

  /**
   * Helper method for logging that an event occurred for a Hive table and some
   * partitions.
   * @param event The event
   * @param hiveTable The Hive table associated with the event
   * @param partitionIterator An iterator associated with the Hive table
   */
  private static void logPartitionsEvent(ListenerEvent event, Table hiveTable,
                                         Iterator<Partition> partitionIterator)
  {
    Preconditions.checkNotNull(hiveTable);
    String tableName = Preconditions.checkNotNull(hiveTable.getTableName());
    Preconditions.checkNotNull(partitionIterator);
    if (partitionIterator.hasNext())
    {
      // The number of partitions might be large- log each partition separately
      // to be safe.
      partitionIterator.forEachRemaining((partition) ->
        log.info(String.format("SnowflakeHiveListener: %s received for table " +
                                   "'%s' and partition with values (%s)",
                               event.getClass().getSimpleName(),
                               tableName,
                               String.join(", ", partition.getValues()))));
    }
    else
    {
      log.info(String.format("SnowflakeHiveListener: %s received for " +
                                 "table '%s' and no partitions",
                             event.getClass().getSimpleName(),
                             tableName));
    }
  }
}
