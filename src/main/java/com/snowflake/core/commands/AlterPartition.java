/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.snowflake.conf.SnowflakeConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;

import java.lang.UnsupportedOperationException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * A class for the AlterPartition command
 * @author wwong
 */
public class AlterPartition implements Command
{
  /**
   * Creates an AlterPartition command
   * @param alterPartitionEvent Event to generate a command from
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  public AlterPartition(AlterPartitionEvent alterPartitionEvent,
                        SnowflakeConf snowflakeConf)
  {
    // Unlike add partition events, alter partition events come in one by one
    Preconditions.checkNotNull(alterPartitionEvent);
    this.hiveTable = Preconditions.checkNotNull(alterPartitionEvent.getTable());
    this.oldPartition =
        Preconditions.checkNotNull(alterPartitionEvent.getOldPartition());
    this.newPartition =
        Preconditions.checkNotNull(alterPartitionEvent.getNewPartition());
    this.snowflakeConf = Preconditions.checkNotNull(snowflakeConf);
    this.hiveConf = Preconditions.checkNotNull(
        alterPartitionEvent.getHandler().getConf());
  }

  /**
   * Generates the necessary commands on a Hive alter partition event
   * @return The Snowflake commands generated
   * @throws SQLException Thrown when there was an error executing a Snowflake
   *                      SQL command.
   * @throws UnsupportedOperationException Thrown when the input is invalid
   */
  public List<String> generateCommands()
      throws SQLException, UnsupportedOperationException
  {
    // In contrast to Hive, adding existing partitions to Snowflake will not
    // result in an error. Instead, it updates the file registration.
    Supplier<Iterator<Partition>> partitionIterator =
        () -> Collections.singletonList(newPartition).iterator();
    return ImmutableList.<String>builder()
        .addAll(new CreateExternalTable(hiveTable,
                                        snowflakeConf,
                                        hiveConf,
                                        false).generateCommands())
        .addAll(new AddPartition(hiveTable,
                                 partitionIterator).generateCommands())
        .build();
  }

  private final Table hiveTable;

  private final Partition oldPartition;

  private final Partition newPartition;

  private final Configuration hiveConf;

  private final SnowflakeConf snowflakeConf;
}
