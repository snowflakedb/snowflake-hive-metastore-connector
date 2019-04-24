/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.snowflake.core.util.StringUtil;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * A class for the AddPartition command
 * @author wwong
 */
public class AddPartition implements Command
{
  /**
   * Creates an AddPartition command
   * @param addPartitionEvent Event to generate a command from
   */
  public AddPartition(AddPartitionEvent addPartitionEvent)
  {
    Preconditions.checkNotNull(addPartitionEvent);
    this.hiveTable = Preconditions.checkNotNull(addPartitionEvent.getTable());
    this.getPartititonsIterator = addPartitionEvent::getPartitionIterator;
  }

  /**
   * Creates an AddPartition command without an event
   * @param hiveTable The Hive table to generate a command from
   * @param getPartititonsIterator A method that supplies an iterator for
   *                               partitions to add
   */
  protected AddPartition(Table hiveTable,
                         Supplier<Iterator<Partition>> getPartititonsIterator)
  {
    this.hiveTable = hiveTable;
    this.getPartititonsIterator = getPartititonsIterator;
  }

  /**
   * Generates the commands for add partition.
   * Note: the partition location must be a subpath of the stage location
   * @param partition Partition object to generate a command from
   * @return The equivalent Snowflake command generated, for example:
   *         ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='partcolname')
   *         LOCATION 'sub/path'
   *         /* TABLE LOCATION = 's3n://bucketname/path/to/table' * /;
   */
  private String generateAddPartitionCommand(Partition partition)
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
        return new LogCommand(
            "Cannot add partition __HIVE_DEFAULT_PARTITION__. Skipping.").generateCommands().get(0);
      }

      partitionDefinitions.add(
          String.format("%1$s='%2$s'",
                        StringUtil.escapeSqlIdentifier(partitionKeys.get(i).getName()),
                        StringUtil.escapeSqlText(partitionValues.get(i))));
    }

    return String.format(
        "ALTER EXTERNAL TABLE %1$s " +
        "ADD PARTITION(%2$s) " +
        "LOCATION '%3$s' " +
        "/* TABLE LOCATION = '%4$s' */;",
        StringUtil.escapeSqlIdentifier(hiveTable.getTableName()),
        String.join(",", partitionDefinitions),
        StringUtil.escapeSqlText(StringUtil.relativizePartitionURI(hiveTable, partition)),
        StringUtil.escapeSqlComment(hiveTable.getSd().getLocation()));
  }

  /**
   * Generates the commands for add partition.
   * @return The Snowflake commands generated
   */
  public List<String> generateCommands()
  {
    List<String> queryList = new ArrayList<>();

    Iterator<Partition> partitionIterator = this.getPartititonsIterator.get();
    while (partitionIterator.hasNext())
    {
      Partition partition = partitionIterator.next();
      queryList.add(this.generateAddPartitionCommand(partition));
    }

    return queryList;
  }

  private final Table hiveTable;

  private final Supplier<Iterator<Partition>> getPartititonsIterator;
}
