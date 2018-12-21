/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.jdbc.client;

import com.snowflake.conf.SnowflakeJdbcConf;
import com.snowflake.core.commands.Command;
import com.snowflake.core.util.CommandGenerator;
import com.snowflake.hive.listener.SnowflakeHiveListener;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * Class that uses the snowflake jdbc to connect to snowflake.
 * Executes the commands
 * TODO: modify exception handling
 */
public class SnowflakeClient
{

  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeHiveListener.class);

  /**
   * Creates and executes an event for snowflake. The steps are:
   * 1. Generate the list of Snowflake queries that will need to be run given
   *    the Hive command
   * 2. Get the connection to a Snowflake account
   * 3. Run the query on Snowflake
   * @param event - the hive event
   * @param snowflakeJdbcConf - the configuration for snowflake jdbc
   */
  public static void createAndExecuteEventForSnowflake(
      ListenerEvent event,
      SnowflakeJdbcConf snowflakeJdbcConf)
  {
    // Obtains the proper command
    log.info("Creating the Snowflake command");
    Command command = CommandGenerator.getCommand(event);

    // Generate the string queries for the command
    // Some Hive commands require more than one statement in Snowflake
    // For example, for create table, a stage must be created before the table
    log.info("Generating Snowflake queries");
    List<String> commandList;
    try
    {
      commandList = command.generateCommands();
    }
    catch (Exception e)
    {
      log.error("Could not generate the Snowflake commands: " + e.getMessage());
      return;
    }

    // Get connection
    log.info("Getting connection to the Snowflake");
    // TODO: retry in the case of failure
    try (Connection connection = getConnection(snowflakeJdbcConf))
    {
      commandList.forEach(commandStr ->
      {
        try (Statement statement = connection.createStatement())
        {
          log.info("Executing command: " + commandStr);
          ResultSet resultSet = statement.executeQuery(commandStr);
          StringBuilder sb = new StringBuilder();
          sb.append("Result:\n");
          while (resultSet.next())
          {
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++)
            {
              if (i == resultSet.getMetaData().getColumnCount())
              {
                sb.append(resultSet.getString(i));
              }
              else
              {
                sb.append(resultSet.getString(i) + "|");
              }
            }
            sb.append("\n");
          }
          log.info(sb.toString());
        }
        catch (Exception e)
        {
          log.error("There was an error executing the statement: " +
              e.getMessage());
        }
      });
    }
    catch (java.sql.SQLException e)
    {
      log.error("There was an error creating the query: " +
                e.getMessage());
      return;
    }
  }

  /**
   * Get the connection to the snowflake account.
   * First finds a snowflake driver and connects to Snowflake using the
   * given properties
   * @param snowflakeJdbcConf - the configuration for snowflake jdbc
   * @return
   * @throws SQLException
   */
  private static Connection getConnection(SnowflakeJdbcConf snowflakeJdbcConf)
      throws SQLException
  {
    try
    {
      Class.forName("com.snowflake.jdbc.client.SnowflakeClient");
    }
    catch(ClassNotFoundException e)
    {
      log.error("Driver not found");
    }

    // build connection properties
    Properties properties = new Properties();

    snowflakeJdbcConf.forEach(conf ->
      {
        SnowflakeJdbcConf.ConfVars confVar =
            SnowflakeJdbcConf.ConfVars.findByName(conf.getKey());
        if (confVar == null)
        {
          properties.put(conf.getKey(), conf.getValue());
        }
        else
        {
          properties.put(confVar.getSnowflakePropertyName(), conf.getValue());
        }

      });
    String connectStr = snowflakeJdbcConf.get(
        SnowflakeJdbcConf.ConfVars.SNOWFLAKE_JDBC_CONNECTION.getVarname());

    return DriverManager.getConnection(connectStr, properties);
  }
}
