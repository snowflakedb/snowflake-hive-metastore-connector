/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.snowflake.core.util.StringUtil.SensitiveString;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class for the DropExternalTable command
 */
public class DropExternalTable implements Command
{

  public DropExternalTable(DropTableEvent dropTableEvent)
  {
    Preconditions.checkNotNull(dropTableEvent);
    this.hiveTable = Preconditions.checkNotNull(dropTableEvent.getTable());
  }

  /**
   * Generates the command for drop external table
   * @return The Snowflake command generated, for example:
   *         DROP EXTERNAL TABLE IF EXISTS T1;
   */
  private String generateDropTableCommand()
  {
    StringBuilder sb = new StringBuilder();

    // drop table command
    sb.append("DROP EXTERNAL TABLE IF EXISTS ");
    sb.append(hiveTable.getTableName());
    sb.append(";");

    return sb.toString();
  }

  /**
   * Generates the command for drop stage
   * @return The Snowflake command generated, for example:
   *         DROP STAGE IF EXISTS S1;
   */
  private String generateDropStageCommand()
  {
    StringBuilder sb = new StringBuilder();

    // drop stage command
    sb.append("DROP STAGE IF EXISTS ");
    sb.append(hiveTable.getTableName());
    sb.append(";");

    return sb.toString();
  }

  /**
   * Generates the necessary commands on a hive drop table event
   * @return The Snowflake commands generated
   */
  public List<SensitiveString> generateCommands()
  {
    List<String> queryList = new ArrayList<>();

    String dropTableQuery = generateDropTableCommand();

    queryList.add(dropTableQuery);

    String dropStageQuery = generateDropStageCommand();
    queryList.add(dropStageQuery);

    return queryList
        .stream().map(SensitiveString::new).collect(Collectors.toList());
  }

  private final Table hiveTable;
}
