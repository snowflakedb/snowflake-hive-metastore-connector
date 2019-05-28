/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.hive.listener;

import com.google.common.base.Preconditions;
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.jdbc.client.SnowflakeClient;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

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

  private static Pattern tableNameFilter; // null if there is no filter

  private static Pattern databaseNameFilter; // null if there is no filter

  public SnowflakeHiveListener(Configuration config)
  {
    super(config);
    // generate the snowflake jdbc conf
    snowflakeConf = new SnowflakeConf();
    tableNameFilter = snowflakeConf.getPattern(
        SnowflakeConf.ConfVars.SNOWFLAKE_TABLE_FILTER_REGEX.getVarname(), null);
    databaseNameFilter = snowflakeConf.getPattern(
        SnowflakeConf.ConfVars.SNOWFLAKE_DATABASE_FILTER_REGEX.getVarname(), null);
    log.info("SnowflakeHiveListener created");
  }

  /**
   * The listener for the create table command
   * @param tableEvent An event that was listened for
   */
  @Override
  public void onCreateTable(CreateTableEvent tableEvent)
  {
    logTableEvent("Event received", tableEvent, tableEvent.getTable());
    if (shouldHandle(tableEvent, tableEvent.getTable()))
    {
      SnowflakeClient.createAndExecuteCommandForSnowflake(tableEvent,
                                                          snowflakeConf);
    }
    else
    {
      logTableEvent("Nothing to do", tableEvent, tableEvent.getTable());
    }
  }

  /**
   * The listener for the drop table command
   * @param tableEvent An event that was listened for
   */
  @Override
  public void onDropTable(DropTableEvent tableEvent)
  {
    logTableEvent("Event received", tableEvent, tableEvent.getTable());
    if (shouldHandle(tableEvent, tableEvent.getTable()))
    {
      SnowflakeClient.createAndExecuteCommandForSnowflake(tableEvent,
                                                          snowflakeConf);
    }
    else
    {
      logTableEvent("Nothing to do", tableEvent, tableEvent.getTable());
    }
  }

  /**
   * The listener for the add partition command
   * @param partitionEvent An event that was listened for
   */
  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent)
  {
    logPartitionsEvent("Event received", partitionEvent,
                       partitionEvent.getTable(), partitionEvent.getPartitionIterator());
    if (partitionEvent.getStatus())
    {
      SnowflakeClient.createAndExecuteCommandForSnowflake(partitionEvent,
                                                          snowflakeConf);
    }
    else
    {
      logPartitionsEvent("Nothing to do", partitionEvent,
                         partitionEvent.getTable(), partitionEvent.getPartitionIterator());
    }
  }

  /**
   * The listener for the drop partition command
   * @param partitionEvent An event that was listened for
   */
  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent)
  {
    logPartitionsEvent("Event received", partitionEvent,
                       partitionEvent.getTable(), partitionEvent.getPartitionIterator());
    if (shouldHandle(partitionEvent, partitionEvent.getTable()))
    {
      SnowflakeClient.createAndExecuteCommandForSnowflake(partitionEvent,
                                                          snowflakeConf);
    }
    else
    {
      logPartitionsEvent("Nothing to do", partitionEvent,
                         partitionEvent.getTable(), partitionEvent.getPartitionIterator());
    }
  }

  /**
   * The listener for the alter table command
   * @param tableEvent An event that was listened for
   */
  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException
  {
    logTableEvent("Event received", tableEvent, tableEvent.getNewTable());
    if (shouldHandle(tableEvent, tableEvent.getNewTable()))
    {
      SnowflakeClient.createAndExecuteCommandForSnowflake(tableEvent,
                                                          snowflakeConf);
    }
    else
    {
      logTableEvent("Nothing to do", tableEvent, tableEvent.getNewTable());
    }
  }

  /**
   * The listener for the alter partition command
   * @param partitionEvent An event that was listened for
   */
  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException
  {
    logPartitionEvent("Event received", partitionEvent,
                      partitionEvent.getTable(), partitionEvent.getNewPartition());
    if (shouldHandle(partitionEvent, partitionEvent.getTable()))
    {
      SnowflakeClient.createAndExecuteCommandForSnowflake(partitionEvent,
                                                          snowflakeConf);
    }
    else
    {
      logPartitionEvent("Nothing to do", partitionEvent,
                        partitionEvent.getTable(), partitionEvent.getNewPartition());
    }
  }

  /**
   * Helper method for logging a message related to an event and Hive table
   * @param message The message to log
   * @param event The event
   * @param hiveTable The Hive table associated with the event
   */
  private static void logTableEvent(String message, ListenerEvent event,
                                    Table hiveTable)
  {
    Preconditions.checkNotNull(message);
    Preconditions.checkNotNull(event);
    Preconditions.checkNotNull(hiveTable);
    String tableName = Preconditions.checkNotNull(hiveTable.getTableName());
    log.info(String.format("SnowflakeHiveListener: %s (Event='%s' Table='%s')",
                           message,
                           event.getClass().getSimpleName(),
                           tableName));
  }

  /**
   * Helper method for logging a message related to an event, Hive table,
   * and a single partition.
   * @param message The message to log
   * @param event The event
   * @param hiveTable The Hive table associated with the event
   * @param partition The partition associated with the event
   */
  private static void logPartitionEvent(String message, ListenerEvent event,
                                        Table hiveTable, Partition partition)
  {
    Preconditions.checkNotNull(message);
    Preconditions.checkNotNull(event);
    Preconditions.checkNotNull(hiveTable);
    Preconditions.checkNotNull(partition);
    String tableName = Preconditions.checkNotNull(hiveTable.getTableName());
    List<String> partitionValues = Preconditions.checkNotNull(partition.getValues());
    log.info(String.format(
        "SnowflakeHiveListener: %s (Event='%s' Table='%s' Partition=%s)",
        message,
        event.getClass().getSimpleName(),
        tableName,
        String.join(", ", partitionValues)));
  }

  /**
   * Helper method for logging a message related to an event, Hive table,
   * and some partitions.
   * @param message The message to log
   * @param event The event
   * @param hiveTable The Hive table associated with the event
   * @param partitionIterator An iterator associated with the Hive table
   */
  private static void logPartitionsEvent(String message, ListenerEvent event,
                                         Table hiveTable,
                                         Iterator<Partition> partitionIterator)
  {
    Preconditions.checkNotNull(message);
    Preconditions.checkNotNull(event);
    Preconditions.checkNotNull(hiveTable);
    Preconditions.checkNotNull(partitionIterator);
    String tableName = Preconditions.checkNotNull(hiveTable.getTableName());
    if (partitionIterator.hasNext())
    {
      log.info(String.format(
          "SnowflakeHiveListener: %s (Event='%s' Table='%s')",
          message,
          event.getClass().getSimpleName(),
          tableName));

      // The number of partitions might be large- log each partition separately
      // to be safe.
      partitionIterator.forEachRemaining((partition) ->
                                             logPartitionEvent("continued for partition:", event, hiveTable, partition));
    }
    else
    {
      log.info(String.format(
          "SnowflakeHiveListener: %s (Event='%s' Table='%s' No partitions)",
          message,
          event.getClass().getSimpleName(),
          tableName));
    }
  }

  /**
   * Helper method to determine whether the listener should handle an event
   * @param event The event
   * @param table The Hive table associated with the event
   * @return True if the event should be handled, false otherwise
   */
  private static boolean shouldHandle(ListenerEvent event, Table table)
  {
    if (!event.getStatus())
    {
      logTableEvent("Skip event, as status is false", event, table);
      return false;
    }

    if (tableNameFilter != null && tableNameFilter.matcher(table.getTableName()).matches())
    {
      logTableEvent("Skip event, as table name matched filter",
                    event, table);
      return false;
    }

    if (databaseNameFilter != null && databaseNameFilter.matcher(table.getDbName()).matches())
    {
      logTableEvent("Skip event, as database name did not matched filter",
                    event, table);
      return false;
    }

    return true;
  }
}
