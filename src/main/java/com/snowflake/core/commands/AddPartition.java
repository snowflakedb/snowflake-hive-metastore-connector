/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.snowflake.core.util.StringUtil.SensitiveString;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;

import java.net.URI;
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
    this.hiveTable = addPartitionEvent.getTable();
    this.getPartititonsIterator = addPartitionEvent::getPartitionIterator;
  }

  /**
   * Generates the commands for add partition.
   * @return Partition object to generate a command from
   * @throws Exception Thrown when the input is invalid:
   *                    - when the number of partition keys don't match the
   *                      number of partition values
   *                    - when the partition column is not within the stage
   *                      location
   */
  private List<String> generateAddPartitionCommand(Partition partition)
      throws Exception
  {
    List<String> queryList = new ArrayList<>();

    List<FieldSchema> partitionKeys = this.hiveTable.getPartitionKeys();
    List<String> partitionValues = partition.getValues();
    if (partitionKeys.size() != partitionValues.size())
    {
      throw new Exception(String.format(
          "Invalid number of partition values. Expected: %1$d, actual: %2$d.",
          partitionKeys.size(),
          partitionValues.size()));
    }

    List<String> partitionDefinitions = new ArrayList<>();
    for (int i = 0; i < partitionKeys.size(); i++)
    {
      partitionDefinitions.add(String.format("%1$s='%2$s'",
                                             partitionKeys.get(i).getName(),
                                             partitionValues.get(i)));
    }

    // For partitions, Hive requires absolute paths, while Snowflake requires
    // absolute paths.
    URI relativeLocation =
        URI.create(hiveTable.getSd().getLocation())
        .relativize(URI.create(partition.getSd().getLocation()));
    if (relativeLocation.isAbsolute())
    {
      // Relativizing failed, since relativized URI is still absolute.
      throw new Exception("The stage location must contain the partition " +
                          "location.");
    }

    String addPartitionQuery = String.format(
        "ALTER EXTERNAL TABLE %1$s " +
        "ADD PARTITION(%2$s) " +
        "LOCATION '%3$s';",
        this.hiveTable.getTableName(),
        String.join(",", partitionDefinitions),
        relativeLocation);
    queryList.add(addPartitionQuery);

    return queryList;
  }

  /**
   * Generates the commands for add partition.
   * @return The generated commands
   * @throws Exception Thrown when the input is invalid
   */
  public List<SensitiveString> generateCommands()
      throws Exception
  {
    List<String> queryList = new ArrayList<>();

    Iterator<Partition> partitionIterator = this.getPartititonsIterator.get();
    while (partitionIterator.hasNext())
    {
      Partition partition = partitionIterator.next();
      queryList.addAll(this.generateAddPartitionCommand(partition));
    }

    return queryList
        .stream().map(SensitiveString::new).collect(Collectors.toList());
  }

  private Table hiveTable;

  private Supplier<Iterator<Partition>> getPartititonsIterator;
}
