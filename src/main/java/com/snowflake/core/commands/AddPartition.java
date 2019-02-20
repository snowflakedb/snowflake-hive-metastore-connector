/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.snowflake.core.util.StringUtil;
import com.snowflake.core.util.StringUtil.SensitiveString;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A class for the AddPartition command
 * @author wwong
 */
public class AddPartition implements Command
{
  /**
   * Creates a AddPartition command
   * @param addPartitionEvent Event to generate a command from
   */
  public AddPartition(AddPartitionEvent addPartitionEvent)
  {
    Preconditions.checkNotNull(addPartitionEvent);
    this.hiveTable = Preconditions.checkNotNull(addPartitionEvent.getTable());
    this.getPartititonsIterator = addPartitionEvent::getPartitionIterator;
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
      partitionDefinitions.add(String.format("%1$s='%2$s'",
                                             partitionKeys.get(i).getName(),
                                             partitionValues.get(i)));
    }

    return String.format(
        "ALTER EXTERNAL TABLE %1$s " +
        "ADD PARTITION(%2$s) " +
        "LOCATION '%3$s' " +
        "/* TABLE LOCATION = '%4$s' */;",
        hiveTable.getTableName(),
        String.join(",", partitionDefinitions),
        StringUtil.relativizePartitionURI(hiveTable, partition),
        hiveTable.getSd().getLocation());
  }

  /**
   * Generates the commands for add partition.
   * @return The Snowflake commands generated
   */
  public List<SensitiveString> generateCommands()
  {
    List<String> queryList = new ArrayList<>();

    Iterator<Partition> partitionIterator = this.getPartititonsIterator.get();
    while (partitionIterator.hasNext())
    {
      Partition partition = partitionIterator.next();
      queryList.add(this.generateAddPartitionCommand(partition));
    }

    return queryList
        .stream().map(SensitiveString::new).collect(Collectors.toList());
  }

  private final Table hiveTable;

  private final Supplier<Iterator<Partition>> getPartititonsIterator;
}
