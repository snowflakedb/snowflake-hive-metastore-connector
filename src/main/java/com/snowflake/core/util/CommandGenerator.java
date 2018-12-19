/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.util;

import com.snowflake.core.commands.Command;
import com.snowflake.core.commands.CreateExternalTable;
import com.snowflake.hive.listener.SnowflakeHiveListener;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that generates the commands
 * to be executed
 */
public class CommandGenerator
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeHiveListener.class);

  /**
   * Creates a command based on the arguments
   * Defers the actual creation to subclasses
   * TODO: support more commands
   * @param event - the event passed from the hive metastore
   * @return a command corresponding to the command to be executed
   */
  public static Command getCommand(ListenerEvent event)
  {
    log.info("Get command executed");
    Command command = null;
    if (event instanceof CreateTableEvent)
    {
      log.info("Generating Create Table command");
      command = new CreateExternalTable((CreateTableEvent)event);
    }

    return command;
  }
}