/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.snowflake.core.util.StringUtil;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * A class for the DropPartition command
 * @author wwong
 */
public class DropPartition implements Command
{
  /**
   * Creates a DropPartition command
   * @param dropPartitionEvent Event to generate a command from
   */
  public DropPartition(DropPartitionEvent dropPartitionEvent)
  {
    Preconditions.checkNotNull(dropPartitionEvent);
    this.hiveTable = Preconditions.checkNotNull(dropPartitionEvent.getTable());
    this.getPartititonsIterator = dropPartitionEvent::getPartitionIterator;
  }

  /**
   * Generates the command for drop partitions.
   * Note: Unlike Hive, Snowflake partitions are dropped using locations.
   * @param partition Partition object to generate a command from
   * @return The Snowflake command generated, for example:
   *         ALTER EXTERNAL TABLE t1 DROP PARTITION LOCATION 'location'
   *         /* TABLE LOCATION = 's3n://bucketname/path/to/table' * /;
   */
  private String generateDropPartitionCommand(Partition partition)
  {
    return String.format(
        "ALTER EXTERNAL TABLE %1$s " +
            "DROP PARTITION " +
            "LOCATION '%2$s' " +
            "/* TABLE LOCATION = '%3$s' */;",
        this.hiveTable.getTableName(),
        StringUtil.relativizePartitionURI(hiveTable, partition),
        hiveTable.getSd().getLocation());
  }

  /**
   * Generates the necessary commands on a Hive drop partition event
   * @return The Snowflake commands generated
   */
  public List<String> generateCommands()
  {
    List<String> queryList = new ArrayList<>();

    Iterator<Partition> partitionIterator = this.getPartititonsIterator.get();
    while (partitionIterator.hasNext())
    {
      Partition partition = partitionIterator.next();
      queryList.add(this.generateDropPartitionCommand(partition));
    }

    return queryList;
  }

  private final Table hiveTable;

  private final Supplier<Iterator<Partition>> getPartititonsIterator;
}
