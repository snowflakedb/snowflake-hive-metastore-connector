/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
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
import java.util.function.Supplier;
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
   */
  public AddPartition(AddPartitionEvent addPartitionEvent,
                      SnowflakeConf snowflakeConf)
  {
    this(Preconditions.checkNotNull(addPartitionEvent).getTable(),
         addPartitionEvent::getPartitionIterator,
         snowflakeConf,
         addPartitionEvent.getHandler().getConf());
  }

  /**
   * Creates an AddPartition command
   * @param alterPartitionEvent Event to generate a command from
   */
  public AddPartition(AlterPartitionEvent alterPartitionEvent,
                      SnowflakeConf snowflakeConf)
  {
    this(Preconditions.checkNotNull(alterPartitionEvent).getTable(),
         () -> Iterators.singletonIterator(alterPartitionEvent.getOldPartition()),
         snowflakeConf,
         alterPartitionEvent.getHandler().getConf());
  }

  /**
   * Creates an AddPartition command without an event
   * @param hiveTable The Hive table to generate a command from
   * @param getPartititonsIterator A method that supplies an iterator for
   *                               partitions to add
   */
  protected AddPartition(Table hiveTable,
                         Supplier<Iterator<Partition>> getPartititonsIterator,
                         SnowflakeConf snowflakeConf,
                         Configuration hiveConf)
  {
    super(hiveTable);
    this.hiveTable = Preconditions.checkNotNull(hiveTable);
    this.getPartititonsIterator = Preconditions.checkNotNull(getPartititonsIterator);
    this.snowflakeConf = Preconditions.checkNotNull(snowflakeConf);
    this.hiveConf = Preconditions.checkNotNull(hiveConf);
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
            "Cannot add partition __HIVE_DEFAULT_PARTITION__. Skipping.").generateStatements().get(0);
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
   * Generates the commands for add partition.
   * @return The Snowflake commands generated
   */
  public List<String> generateStatements() throws SQLException
  {
    List<String> queryList = new ArrayList<>(
        new CreateExternalTable(hiveTable,
                                snowflakeConf,
                                hiveConf,
                                false // Do not replace table
                                ).generateStatements());

    Iterators.partition(this.getPartititonsIterator.get(), MAX_ADDED_PARTITIONS)
        .forEachRemaining(partitions ->
            queryList.add(generateAddPartitionsCommand(
                Lists.newArrayList(partitions))));

    return queryList;
  }

  /**
   * Combines two add partition commands into one or two new commands. The
   * first command outputted will contain at most MAX_ADDED_PARTITIONS
   * partitions to add. If there are more than that many commands, the rest will
   * overflow into the second command.
   * If either is null, return a list with the other. Both input commands
   * cannot be null.
   * @param command1 a command to combine
   * @param command2 a command to combine
   * @return one or two "combined" commands
   */
  public static List<AddPartition> combinedOf(AddPartition command1,
                                              AddPartition command2)
  {
    Preconditions.checkState(command1 != null || command2 != null);
    if (command1 == null)
    {
      return ImmutableList.of(command2);
    }
    if (command2 == null)
    {
      return ImmutableList.of(command1);
    }

    Iterator<Partition> combined = Iterators.concat(command1.getPartititonsIterator.get(),
                                                    command2.getPartititonsIterator.get());
    // Creating an array from and iterator also advances the combined one
    Iterator<Partition> first = Iterators.forArray(Iterators.toArray(
        Iterators.limit(combined, MAX_ADDED_PARTITIONS), Partition.class));

    AddPartition firstCommand = new AddPartition(command2.hiveTable,
                                                 () -> first,
                                                 command2.snowflakeConf,
                                                 command2.hiveConf);
    if (!combined.hasNext())
    {
      return ImmutableList.of(firstCommand);
    }

    AddPartition secondCommand = new AddPartition(command2.hiveTable,
                                                  () -> combined,
                                                  command2.snowflakeConf,
                                                  command2.hiveConf);
    return ImmutableList.of(firstCommand, secondCommand);
  }

  private final Table hiveTable;

  private final Supplier<Iterator<Partition>> getPartititonsIterator;

  private final Configuration hiveConf;

  private final SnowflakeConf snowflakeConf;
}
