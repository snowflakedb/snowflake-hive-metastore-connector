/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.snowflake.conf.SnowflakeConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;

import java.lang.UnsupportedOperationException;
import java.sql.SQLException;
import java.util.List;

/**
 * A class for the AlterTable command
 * @author wwong
 */
public class AlterTable implements Command
{
  /**
   * Creates a AlterTable command
   * @param alterTableEvent Event to generate a command from
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  public AlterTable(AlterTableEvent alterTableEvent,
                    SnowflakeConf snowflakeConf)
  {
    Preconditions.checkNotNull(alterTableEvent);
    this.oldHiveTable =
        Preconditions.checkNotNull(alterTableEvent.getOldTable());
    this.newHiveTable =
        Preconditions.checkNotNull(alterTableEvent.getNewTable());
    this.snowflakeConf = Preconditions.checkNotNull(snowflakeConf);
    this.hiveConf = Preconditions.checkNotNull(
        alterTableEvent.getHandler().getConf());
  }

  /**
   * Generates the necessary commands on a Hive alter table event
   * @return The Snowflake commands generated
   * @throws SQLException Thrown when there was an error executing a Snowflake
   *                      SQL command.
   * @throws UnsupportedOperationException Thrown when the input is invalid
   */
  public List<String> generateCommands()
      throws SQLException, UnsupportedOperationException
  {
    // TODO: Add support for other alter table commands, such as add columns
    if (oldHiveTable.getTableName().equals(newHiveTable.getTableName()))
    {
      return new CreateExternalTable(newHiveTable,
                                     snowflakeConf,
                                     hiveConf,
                                     false // Do not replace table
      ).generateCommands();
    }
    else
    {
      return new LogCommand("Received no-op alter table command.").generateCommands();
    }
  }

  private final Table oldHiveTable;

  private final Table newHiveTable;

  private final Configuration hiveConf;

  private final SnowflakeConf snowflakeConf;
}
