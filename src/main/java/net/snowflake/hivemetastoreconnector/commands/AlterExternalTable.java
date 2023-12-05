/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.commands;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.util.HiveToSnowflakeType;
import net.snowflake.hivemetastoreconnector.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;

import java.lang.UnsupportedOperationException;
import java.sql.SQLException;
import java.util.Arrays;
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
   * Generates the command for rename table
   * @return The Snowflake command generated, for example:
   *         ALTER TABLE IF EXISTS T1 RENAME TO T2;
   */
  private String generateAlterTableRenameCommand()
  {
    StringBuilder sb = new StringBuilder();

    // rename table command
    sb.append("ALTER TABLE IF EXISTS ");
    sb.append(StringUtil.escapeSqlIdentifier(oldHiveTable.getTableName()));
    sb.append(" RENAME TO ");
    sb.append(StringUtil.escapeSqlIdentifier(newHiveTable.getTableName()));
    sb.append(";");

    return sb.toString();
  }

  /**
   * Generates the command for rename stage
   * @return The Snowflake command generated, for example:
   *         ALTER STAGE IF EXISTS S1 RENAME TO S2;
   */
  private String generateAlterStageRenameCommand()
  {
    StringBuilder sb = new StringBuilder();

    // rename stage command
    sb.append("ALTER STAGE IF EXISTS ");
    sb.append(CreateExternalTable.generateStageName(oldHiveTable, snowflakeConf));
    sb.append(" RENAME TO ");
    sb.append(CreateExternalTable.generateStageName(newHiveTable, snowflakeConf));
    sb.append(";");

    return sb.toString();
  }

  /**
   * Generates the necessary queries on a hive rename table event
   * @return The Snowflake queries generated
   */
  public List<String> generateAlterTableRanameSqlQueries()
  {
    List<String> queryList = new ArrayList<>();

    String alterTableRenameQuery = generateAlterTableRenameCommand();
    queryList.add(alterTableRenameQuery);

    String alterStageRenameQuery = generateAlterStageRenameCommand();
    queryList.add(alterStageRenameQuery);

    return queryList;
  }

  public List<String> generateAlterStageSetURLSqlQueries()
  {
    return ImmutableList.of(
        String.format("ALTER STAGE IF EXISTS %s SET URL = '%s';",
            CreateExternalTable.generateStageName(newHiveTable, snowflakeConf),
            newHiveTable.getSd().getLocation()));
  }

  public List<String> generateRefreshTableSqlQueries()
  {
      return ImmutableList.of(
         String.format("ALTER EXTERNAL TABLE IF EXISTS %s REFRESH;",
             StringUtil.escapeSqlIdentifier(newHiveTable.getTableName())));
  }

  /**
   * Generates the necessary queries on a Hive alter table event
   * @return The Snowflake queries generated
   * @throws SQLException Thrown when there was an error executing a Snowflake
   *                      SQL query (if a Snowflake query must be executed).
   * @throws UnsupportedOperationException Thrown when the input is invalid
   */
  public List<String> generateSqlQueries()
      throws SQLException, UnsupportedOperationException
  {
    // TODO: Add support for other alter table commands
    if (!oldHiveTable.getTableName().equals(newHiveTable.getTableName()))
    {
      return generateAlterTableRanameSqlQueries();
    }

    List<String> commands = new ArrayList<String>();

    // All supported alter table events (e.g. touch) generate create statements
    commands.addAll(new CreateExternalTable(
        oldHiveTable,
        snowflakeConf,
        hiveConf,
        false // Do not replace table
    ).generateSqlQueries());

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

    if (!Objects.equal(oldHiveTable.getSd().getLocation(), newHiveTable.getSd().getLocation())) {
      commands.addAll(generateAlterStageSetURLSqlQueries());
      commands.addAll(generateRefreshTableSqlQueries());
    }

    return commands;
  }

  private final Table oldHiveTable;

  private final Table newHiveTable;

  private final Configuration hiveConf;

  private final SnowflakeConf snowflakeConf;
}
