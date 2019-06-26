/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.core;

import net.snowflake.hivemetastoreconnector.commands.AddPartition;
import net.snowflake.hivemetastoreconnector.commands.AlterExternalTable;
import net.snowflake.hivemetastoreconnector.commands.Command;
import net.snowflake.hivemetastoreconnector.commands.CreateExternalTable;
import net.snowflake.hivemetastoreconnector.commands.DropExternalTable;
import net.snowflake.hivemetastoreconnector.commands.DropPartition;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.SnowflakeHiveListener;
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
 * A class that generates the commands to be executed
 */
public class CommandGenerator
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeHiveListener.class);

  /**
   * Creates a command based on the arguments
   * Defers the actual creation to subclasses
   * @param event - the event passed from the hive metastore
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   * @return a command corresponding to the command to be executed
   */
  public static Command getCommand(ListenerEvent event,
                                   SnowflakeConf snowflakeConf)
  {
    log.info(String.format("Get command executed (%s)",
                           event.getClass().getSimpleName()));
    Command command = null;
    if (event instanceof CreateTableEvent)
    {
      log.info("Generating Create Table command");
      command = new CreateExternalTable((CreateTableEvent)event, snowflakeConf);
    }
    else if (event instanceof DropTableEvent)
    {
      log.info("Generating Drop Table command");
      command = new DropExternalTable((DropTableEvent)event, snowflakeConf);
    }
    else if (event instanceof AddPartitionEvent)
    {
      log.info("Generating Add Partition command");
      command = new AddPartition((AddPartitionEvent)event, snowflakeConf);
    }
    else if (event instanceof DropPartitionEvent)
    {
      log.info("Generating Drop Partition command");
      command = new DropPartition((DropPartitionEvent)event);
    }
    else if (event instanceof AlterTableEvent)
    {
      log.info("Generating Alter Table command");
      command = new AlterExternalTable((AlterTableEvent)event, snowflakeConf);
    }
    else if (event instanceof AlterPartitionEvent)
    {
      log.info("Generating Alter Partition command");
      command = new AddPartition((AlterPartitionEvent)event, snowflakeConf);
    }

    return command;
  }
}