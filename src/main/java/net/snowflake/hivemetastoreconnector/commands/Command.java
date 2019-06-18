/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.commands;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

/**
 * A class that represents a command to generate executable Snowflake
 * statements from
 * @author xma
 */
public abstract class Command
{
  private final String databaseName;

  private final String tableName;

  protected Command(Table table)
  {
    this(Preconditions.checkNotNull(table).getDbName(),
         table.getTableName());
  }

  protected Command(String databaseName, String tableName)
  {
    this.databaseName = databaseName;
    this.tableName = tableName;
  }

  public String getDatabaseName()
  {
    return databaseName;
  }

  public String getTableName()
  {
    return tableName;
  }

  /**
   * Generates the query in a string form to be sent to Snowflake
   * @return The Snowflake queries generated
   * @throws Exception Thrown when there is an error generating SQL queries
   */
  public abstract List<String> generateSqlQueries() throws Exception;
}
