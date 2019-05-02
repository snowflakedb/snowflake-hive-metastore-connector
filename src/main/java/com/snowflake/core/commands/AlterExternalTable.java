/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.core.util.HiveToSnowflakeType;
import com.snowflake.core.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;

import java.lang.UnsupportedOperationException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A class for the AlterExternalTable command
 * TODO: Split into TouchExternalTable and AlterColumnsExternalTable
 * @author wwong
 */
public class AlterExternalTable extends Command
{
  /**
   * Creates a AlterTable command
   * @param alterTableEvent Event to generate a command from
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  public AlterExternalTable(AlterTableEvent alterTableEvent,
                            SnowflakeConf snowflakeConf)
  {
    super(Preconditions.checkNotNull(alterTableEvent).getOldTable());
    this.oldHiveTable =
        Preconditions.checkNotNull(alterTableEvent.getOldTable());
    this.newHiveTable =
        Preconditions.checkNotNull(alterTableEvent.getNewTable());
    this.snowflakeConf = Preconditions.checkNotNull(snowflakeConf);
    this.hiveConf = Preconditions.checkNotNull(
        alterTableEvent.getHandler().getConf());
  }

  /**
   * Generates commands to add columns to a Snowflake table.
   * @param hiveTable The Hive table to generate a command from
   * @param columns The columns to be added
   * @param startingPosition (CSV file formats only) number of columns to
   *                         assume the table will have
   * @param snowflakeConf The configuration for Snowflake Hive metastore
   *                      listener
   * @return The commands generated, for example:
   *         ALTER TABLE t1 ADD COLUMN c1 INT as (VALUE:c1::INT);
   */
  private static List<String> generateAddColumnsCommand(Table hiveTable,
                                                        List<FieldSchema> columns,
                                                        int startingPosition,
                                                        SnowflakeConf snowflakeConf)
  {
    Preconditions.checkNotNull(hiveTable);
    Preconditions.checkNotNull(columns);
    Preconditions.checkArgument(!columns.isEmpty());

    HiveToSnowflakeType.SnowflakeFileFormatType sfFileFmtType =
        HiveToSnowflakeType.toSnowflakeFileFormatType(
            hiveTable.getSd().getSerdeInfo().getSerializationLib(),
            hiveTable.getSd().getInputFormat());

    List<String> columnDefList = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++)
    {
      FieldSchema col = columns.get(i);
      columnDefList.add(CreateExternalTable.generateColumnStr(
          col,
          startingPosition + i,
          sfFileFmtType,
          snowflakeConf));
    }

    return ImmutableList.of(
        String.format(
            "ALTER TABLE %s ADD COLUMN %s;",
            StringUtil.escapeSqlIdentifier(hiveTable.getTableName()),
            String.join(", COLUMN ", columnDefList)));
  }

  /**
   * Generates commands to drop columns from a Snowflake table.
   * @param hiveTable The Hive table to generate a command from
   * @param columns The names of columns to drop
   * @return The commands generated, for example:
   *         ALTER TABLE t1 DROP COLUMN c1;
   */
  private static List<String> generateDropColumnsCommand(Table hiveTable,
                                                         List<String> columns)
  {
    Preconditions.checkNotNull(hiveTable);
    Preconditions.checkNotNull(columns);
    Preconditions.checkArgument(!columns.isEmpty());

    return ImmutableList.of(
        String.format(
            "ALTER TABLE %s DROP COLUMN %s;",
            StringUtil.escapeSqlIdentifier(hiveTable.getTableName()),
            String.join(", ",
                        columns.stream()
                            .map(StringUtil::escapeSqlIdentifier)
                            .collect(Collectors.toList()))));
  }

  /**
   * Generates the necessary commands on a Hive alter table event
   * @return The Snowflake commands generated
   * @throws SQLException Thrown when there was an error executing a Snowflake
   *                      SQL command.
   * @throws UnsupportedOperationException Thrown when the input is invalid
   */
  public List<String> generateStatements()
      throws SQLException, UnsupportedOperationException
  {
    // TODO: Add support for other alter table commands, such as rename table
    if (!oldHiveTable.getTableName().equals(newHiveTable.getTableName()))
    {
      return new LogCommand(oldHiveTable, "Received no-op alter table command.").generateStatements();
    }

    // All supported alter table events (e.g. touch) generate create statements
    List<String> commands = new CreateExternalTable(
        oldHiveTable,
        snowflakeConf,
        hiveConf,
        false // Do not replace table
    ).generateStatements();

    // If the columns are different, generate an add/drop column event
    // TODO: Support more alter column events, including positional ones
    // TODO: Make add/drop column events idempotent
    // TODO: preserve column order
    Set<String> columnNamesBefore = oldHiveTable.getSd().getCols()
        .stream().map(FieldSchema::getName).collect(Collectors.toSet());
    Set<String> columnNamesAfter = newHiveTable.getSd().getCols()
        .stream().map(FieldSchema::getName).collect(Collectors.toSet());
    if (columnNamesBefore.equals(columnNamesAfter))
    {
      return commands;
    }

    // Set does not guarantee order. Preserve column order to be deterministic
    List<FieldSchema> addedColumns = newHiveTable.getSd().getCols().stream()
        .filter(col -> !columnNamesBefore.contains(col.getName()))
        .collect(Collectors.toList());
    List<String> droppedColumns = oldHiveTable.getSd().getCols().stream()
        .map(FieldSchema::getName)
        .filter(col -> !columnNamesAfter.contains(col))
        .collect(Collectors.toList());
    if (droppedColumns.isEmpty())
    {
      int startingPosition = oldHiveTable.getSd().getCols().size();
      commands.addAll(generateAddColumnsCommand(newHiveTable,
                                                addedColumns,
                                                startingPosition,
                                                snowflakeConf));
    }
    else if (addedColumns.isEmpty())
    {
      commands.addAll(generateDropColumnsCommand(newHiveTable, droppedColumns));
    }
    else
    {
      // Rather than calculate which columns were dropped or added, just drop
      // and add all columns
      commands.addAll(generateDropColumnsCommand(newHiveTable, droppedColumns));
      commands.addAll(generateAddColumnsCommand(newHiveTable,
                                                addedColumns,
                                                0,
                                                snowflakeConf));
    }

    return commands;
  }

  private final Table oldHiveTable;

  private final Table newHiveTable;

  private final Configuration hiveConf;

  private final SnowflakeConf snowflakeConf;
}
