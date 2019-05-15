/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.snowflake.core.util.StringUtil;
import com.google.common.collect.Iterators;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A class for the DropPartition command
 * @author wwong
 */
public class DropPartition extends Command
{
  /**
   * Creates a DropPartition command
   * @param dropPartitionEvent Event to generate a command from
   */
  public DropPartition(DropPartitionEvent dropPartitionEvent)
  {
    super(Preconditions.checkNotNull(dropPartitionEvent).getTable());
    this.hiveTable = Preconditions.checkNotNull(dropPartitionEvent.getTable());
    // Avoid 'this' constructor due to usage of hiveTable below
    this.partititonLocationsIterator = Iterators.transform(
        dropPartitionEvent.getPartitionIterator(),
        partition -> StringUtil.relativizePartitionURI(
            hiveTable,
            Preconditions.checkNotNull(partition)));
  }

  /**
   * Creates a DropPartition command
   * @param hiveTable The Hive table to generate a command from
   * @param partititonLocationsIterator iterator of the locations of the
   *                                    partitions to drop
   */
  public DropPartition(Table hiveTable, Iterator<String> partititonLocationsIterator)
  {
    super(hiveTable);
    this.hiveTable = Preconditions.checkNotNull(hiveTable);
    this.partititonLocationsIterator =
        Preconditions.checkNotNull(partititonLocationsIterator);
  }

  /**
   * Generates the command for drop partitions.
   * Note: Unlike Hive, Snowflake partitions are dropped using locations.
   * @param partitionLocation Partition location to generate a command from
   * @return The Snowflake command generated, for example:
   *         ALTER EXTERNAL TABLE t1 DROP PARTITION LOCATION 'location'
   *         /* TABLE LOCATION = 's3n://bucketname/path/to/table' * /;
   */
  private String generateDropPartitionCommand(String partitionLocation)
  {
    return String.format(
        "ALTER EXTERNAL TABLE %1$s " +
            "DROP PARTITION " +
            "LOCATION '%2$s' " +
            "/* TABLE LOCATION = '%3$s' */;",
        StringUtil.escapeSqlIdentifier(hiveTable.getTableName()),
        StringUtil.escapeSqlText(partitionLocation),
        StringUtil.escapeSqlComment(hiveTable.getSd().getLocation()));
  }

  /**
   * Generates the necessary commands on a Hive drop partition event
   * @return The Snowflake commands generated
   */
  public List<String> generateSqlQueries()
  {
    List<String> queryList = new ArrayList<>();

    while (partititonLocationsIterator.hasNext())
    {
      queryList.add(
          this.generateDropPartitionCommand(partititonLocationsIterator.next()));
    }

    return queryList;
  }

  private final Table hiveTable;

  private final Iterator<String> partititonLocationsIterator;
}
