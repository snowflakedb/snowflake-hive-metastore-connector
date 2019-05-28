/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.core.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class for the AddPartition command
 * @author wwong
 */
public class AddPartition extends Command
{
  // The maximum number of partitions we should add in one statement.
  private static final int MAX_ADDED_PARTITIONS = 100;

  /**
   * Creates an AddPartition command
   * @param addPartitionEvent Event to generate a command from
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  public AddPartition(AddPartitionEvent addPartitionEvent,
                      SnowflakeConf snowflakeConf)
  {
    this(Preconditions.checkNotNull(addPartitionEvent).getTable(),
         addPartitionEvent.getPartitionIterator(),
         snowflakeConf,
         addPartitionEvent.getHandler().getConf(),
         false);
  }

  /**
   * Creates an AddPartition command
   * @param alterPartitionEvent Event to generate a command from
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  public AddPartition(AlterPartitionEvent alterPartitionEvent,
                      SnowflakeConf snowflakeConf)
  {
    this(Preconditions.checkNotNull(alterPartitionEvent).getTable(),
         Iterators.singletonIterator(alterPartitionEvent.getOldPartition()),
         snowflakeConf,
         alterPartitionEvent.getHandler().getConf(),
         false);
  }

  /**
   * Creates an AddPartition command without an event
   * @param hiveTable The Hive table to generate a command from
   * @param partitionsIterator A method that supplies an iterator for
   *                            partitions to add
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   * @param hiveConf The Hive configuration
   * @param isCompact internal marker to check if this command was generated
   *                  by compaction
   */
  protected AddPartition(Table hiveTable,
                         Iterator<Partition> partitionsIterator,
                         SnowflakeConf snowflakeConf,
                         Configuration hiveConf,
                         boolean isCompact)
  {
    super(hiveTable);
    this.hiveTable = Preconditions.checkNotNull(hiveTable);
    this.partitionsIterator = Preconditions.checkNotNull(partitionsIterator);
    this.snowflakeConf = Preconditions.checkNotNull(snowflakeConf);
    this.hiveConf = Preconditions.checkNotNull(hiveConf);
    this.isCompact = isCompact;
  }

  /**
   * Generates the commands for add partition.
   * Note: the partition location must be a subpath of the stage location
   * @param partitions Partition objects to generate a command from
   * @return The equivalent Snowflake command generated, for example:
   *         ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='partcolname')
   *         LOCATION 'sub/path'
   *         /* TABLE LOCATION = 's3n://bucketname/path/to/table' * /;
   */
  private String generateAddPartitionsCommand(List<Partition> partitions)
  {
    Preconditions.checkNotNull(partitions);
    Preconditions.checkArgument(!partitions.isEmpty());
    return String.format(
        "ALTER EXTERNAL TABLE %s ADD %s /* TABLE LOCATION = '%s' */;",
        StringUtil.escapeSqlIdentifier(hiveTable.getTableName()),
        String.join(", ",
                    partitions.stream()
                        .map(this::generatePartitionDetails)
                        .collect(Collectors.toList())),
        StringUtil.escapeSqlComment(hiveTable.getSd().getLocation()));
  }

  /**
   * Generates a portion of the add partition command.
   * @param partition Partition object to generate a portion of a command
   * @return A portion of the command that represents the partition,
   *         for example: PARTITION(partcol='partcolname') LOCATION 'sub/path'
   */
  private String generatePartitionDetails(Partition partition)
  {
    List<FieldSchema> partitionKeys = hiveTable.getPartitionKeys();
    List<String> partitionValues = partition.getValues();
    Preconditions.checkArgument(
        partitionKeys.size() == partitionValues.size(),
        "Invalid number of partition values. Expected: %1$d, actual: %2$d.",
        partitionKeys.size(),
        partitionValues.size());

    List<String> partitionDefinitions = new ArrayList<>();
    for (int i = 0; i < partitionKeys.size(); i++)
    {
      // Snowflake does not allow __HIVE_DEFAULT_PARTITION__ for all data types,
      // skip this partition instead.
      if ("__HIVE_DEFAULT_PARTITION__".equalsIgnoreCase(partitionValues.get(i)))
      {
        return new LogCommand(hiveTable,
            "Cannot add partition __HIVE_DEFAULT_PARTITION__. Skipping.").generateSqlQueries().get(0);
      }

      partitionDefinitions.add(
          String.format("%1$s='%2$s'",
                        StringUtil.escapeSqlIdentifier(partitionKeys.get(i).getName()),
                        StringUtil.escapeSqlText(partitionValues.get(i))));
    }

    return String.format("PARTITION(%s) LOCATION '%s'",
                         String.join(",", partitionDefinitions),
                         StringUtil.escapeSqlText(StringUtil.relativizePartitionURI(hiveTable, partition)));
  }

  /**
   * Generates the queries for add partition.
   * @return The Snowflake queries generated
   */
  public List<String> generateSqlQueries() throws SQLException
  {
    List<String> queryList = new ArrayList<>(
        new CreateExternalTable(hiveTable,
                                snowflakeConf,
                                hiveConf,
                                false // Do not replace table
                                ).generateSqlQueries());

    Iterators.partition(this.partitionsIterator, MAX_ADDED_PARTITIONS)
        .forEachRemaining(partitions ->
            queryList.add(generateAddPartitionsCommand(
                new ArrayList<>(partitions))));

    return queryList;
  }

  /**
   * Combines N add partition commands with M total partitions into
   * ceil(M / MAX_ADDED_PARTITIONS) new add partition commands. If
   * M > N * MAX_ADDED_PARTITIONS, then the result will be more than N elements.
   * Each command except the last one will have MAX_ADDED_PARTITIONS partitions.
   * Assumes that the commands are all for the same table, with no
   * modifications to the table in between
   * @param addPartitions the add partition commands to combine.
   * @return the compacted add partition commands
   */
  public static List<AddPartition> compact(List<AddPartition> addPartitions)
  {
    Preconditions.checkNotNull(addPartitions);
    Preconditions.checkArgument(!addPartitions.isEmpty());

    AddPartition first = addPartitions.get(0);

    List<AddPartition> compacted = new ArrayList<>();

    // Compaction entails:
    // 1. Combine each iterator in to one big iterator
    // 2. Partitioning the combined iterator into sections
    // 3. Creating a command for each section
    Iterators.partition(
        Iterators.concat(
            addPartitions.stream()
                .map(command -> command.partitionsIterator)
                .collect(Collectors.toList()).iterator()),
        MAX_ADDED_PARTITIONS)
        .forEachRemaining(partitions ->
                              compacted.add(
                                  new AddPartition(first.hiveTable,
                                                   partitions.iterator(),
                                                   first.snowflakeConf,
                                                   first.hiveConf,
                                                   true)));

    return compacted;
  }

  /**
   * Mthod to determine whether a command was already compacted
   * @return whether a command was compacted
   */
  public boolean isCompact()
  {
    return isCompact;
  }

  private final Table hiveTable;

  private final Iterator<Partition> partitionsIterator;

  private final Configuration hiveConf;

  private final SnowflakeConf snowflakeConf;

  private final boolean isCompact;
}
