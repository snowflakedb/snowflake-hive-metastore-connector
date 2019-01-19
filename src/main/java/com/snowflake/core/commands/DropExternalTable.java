/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

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
    this.hiveTable = dropTableEvent.getTable();
  }

  /**
   * Generates the command for drop external table
   * @return The generated command
   */
  private String generateDropTableCommand()
  {
    StringBuilder sb = new StringBuilder();

    // drop table command
    sb.append("DROP EXTERNAL TABLE ");
    sb.append(hiveTable.getTableName());
    sb.append(";");

    return sb.toString();
  }

  /**
   * Generates the command for drop stage
   * @return The generated command
   * @throws Exception Thrown when the input is invalid
   */
  private String generateDropStageCommand()
    throws Exception
  {
    StringBuilder sb = new StringBuilder();

    // drop stage command
    sb.append("DROP STAGE ");
    sb.append(hiveTable.getTableName());
    sb.append(";");

    return sb.toString();
  }

  /**
   * Generates the necessary commands on a hive drop table event
   */
  public List<SensitiveString> generateCommands()
    throws Exception
  {
    List<String> queryList = new ArrayList<>();

    String dropTableQuery = generateDropTableCommand();

    queryList.add(dropTableQuery);

    String dropStageQuery = generateDropStageCommand();
    queryList.add(dropStageQuery);

    return queryList
        .stream().map(SensitiveString::new).collect(Collectors.toList());
  }

  private Table hiveTable;
}
