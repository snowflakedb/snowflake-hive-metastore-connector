/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.jdbc.client;

import com.snowflake.conf.SnowflakeConf;
import com.snowflake.core.commands.Command;
import com.snowflake.core.commands.LogCommand;
import com.snowflake.core.util.CommandGenerator;
import com.snowflake.core.util.BatchScheduler;
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
import java.util.Queue;

/**
 * Class that uses the snowflake jdbc to connect to snowflake.
 * Executes the commands
 * TODO: modify exception handling
 */
public class SnowflakeClient
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeHiveListener.class);

  private static BatchScheduler<List<String>> scheduler;

  /**
   * Creates and executes an event for snowflake. The steps are:
   * 1. Generate the list of Snowflake queries that will need to be run given
   *    the Hive command
   * 2. Queue a query to Snowflake
   *
   * At a predetermined interval, the queued queries will be executed.
   * This includes:
   * 1. Batching similar queries
   * 2. Get the connection to a Snowflake account
   * 3. Run the queries on Snowflake
   * @param event - the hive event
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  public static void createAndExecuteEventForSnowflake(
      ListenerEvent event,
      SnowflakeConf snowflakeConf)
  {
    // Obtains the proper command
    log.info("Creating the Snowflake command");
    Command command = CommandGenerator.getCommand(event, snowflakeConf);

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

      // Log a message to Snowflake with the error instead
      commandList = new LogCommand(e).generateCommands();
    }

    boolean backgroundTaskEnabled = !snowflakeConf.getBoolean(
        SnowflakeConf.ConfVars.SNOWFLAKE_CLIENT_FORCE_SYNCHRONOUS.getVarname(), false);
    if (backgroundTaskEnabled)
    {
      initScheduler(snowflakeConf);
      scheduler.enqueueMessage(commandList);
    }
    else
    {
      executeStatements(commandList, snowflakeConf);
    }
  }

  /**
   * Helper method to connect to Snowflake and execute a list of queries
   * @param commandList - The list of queries to execute
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  public static void executeStatements(List<String> commandList,
                                        SnowflakeConf snowflakeConf)
  {
    log.info("Executing statements: " + String.join(", ", commandList));

    // Get connection
    log.info("Getting connection to the Snowflake");
    try (Connection connection = retry(
        () -> getConnection(snowflakeConf), snowflakeConf))
    {
      commandList.forEach(commandStr ->
      {
        try (Statement statement =
            retry(connection::createStatement, snowflakeConf))
        {
          log.info("Executing command: " + commandStr);
          ResultSet resultSet = retry(
              () -> statement.executeQuery(commandStr), snowflakeConf);
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
                sb.append(resultSet.getString(i));
                sb.append("|");
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
    }
  }

  /**
   * (Deprecated)
   * Utility method to connect to Snowflake and execute a query.
   * @param commandStr - The query to execute
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   * @return The result of the executed query
   * @throws SQLException Thrown if there was an error executing the
   *                      statement or forming a connection.
   */
  public static ResultSet executeStatement(String commandStr,
                                           SnowflakeConf snowflakeConf)
      throws SQLException
  {
    try (Connection connection = retry(() -> getConnection(snowflakeConf),
                                       snowflakeConf);
        Statement statement = retry(connection::createStatement,
                                    snowflakeConf))
    {
      log.info("Executing command: " + commandStr);
      ResultSet resultSet = retry(() -> statement.executeQuery(commandStr),
                                  snowflakeConf);
      log.info("Command successfully executed");
      return resultSet;
    }
    catch (SQLException e)
    {
      log.info("There was an error executing this statement or forming a " +
                   "connection: " + e.getMessage());
      throw e;
    }
  }

  /**
   * Helper method. Initializes and starts the query scheduler
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  private static void initScheduler(SnowflakeConf snowflakeConf)
  {
    if (scheduler != null)
    {
      return;
    }

    int numThreads = snowflakeConf.getInt(
        SnowflakeConf.ConfVars.SNOWFLAKE_CLIENT_THREAD_COUNT.getVarname(), 8);

    int batchingPeriod = snowflakeConf.getInt(
        SnowflakeConf.ConfVars.SNOWFLAKE_CLIENT_BATCHING_PERIOD.getVarname(), 1000);

    scheduler = new BatchScheduler<>(numThreads, batchingPeriod,
        (q, s) -> processMessages(q, s, snowflakeConf));
  }

  /**
   * Method that is periodically invoked the by the batch scheduler
   * @param commandQueue Queue containing messages
   * @param scheduler An available thread pool to submit tasks to
   * @param snowflakeConf The Snowflake configuration
   */
  private static void processMessages(Queue<List<String>> commandQueue,
                                      BatchScheduler<List<String>> scheduler,
                                      SnowflakeConf snowflakeConf)
  {
    while(!commandQueue.isEmpty())
    {
      List<String> commandList = commandQueue.remove();

      // TODO: Batch requests
      scheduler.submitTask(() -> executeStatements(commandList, snowflakeConf));
    }
  }

  /**
   * Get the connection to the Snowflake account.
   * First finds a Snowflake driver and connects to Snowflake using the
   * given properties.
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   * @return The JDBC connection
   * @throws SQLException Exception thrown when initializing the connection
   */
  private static Connection getConnection(SnowflakeConf snowflakeConf)
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

    snowflakeConf.forEach(conf ->
      {
        if (!conf.getKey().startsWith("snowflake.jdbc"))
        {
          return;
        }

        SnowflakeConf.ConfVars confVar =
            SnowflakeConf.ConfVars.findByName(conf.getKey());
        if (confVar == null)
        {
          properties.put(conf.getKey(), conf.getValue());
        }
        else
        {
          properties.put(confVar.getSnowflakePropertyName(), conf.getValue());
        }

      });
    String connectStr = snowflakeConf.get(
        SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_CONNECTION.getVarname());

    return DriverManager.getConnection(connectStr, properties);
  }

  /**
   * Helper interface that represents a Supplier that can throw an exception.
   * @param <T> The type of object returned by the supplier
   * @param <E> The type of exception thrown by the supplier
   */
  @FunctionalInterface
  public interface ThrowableSupplier<T, E extends Throwable>
  {
    T get() throws E;
  }

  /**
   * Helper method for simple retries.
   * Note: The total number of attempts is 1 + retries.
   * @param <T> The type of object returned by the supplier
   * @param <E> The type of exception thrown by the supplier
   * @param method The method to be executed and retried on.
   * @param maxRetries The maximum number of retries.
   * @param timeoutInMilliseconds Time between retries.
   */
  private static <T, E extends Throwable> T retry(
      ThrowableSupplier<T,E> method,
      int maxRetries,
      int timeoutInMilliseconds)
  throws E
  {
    // Attempt to call the method with N-1 retries
    for (int i = 0; i < maxRetries; i++)
    {
      try
      {
        // Attempt to call the method
        return method.get();
      }
      catch (Exception e)
      {
        // Wait between retries
        try
        {
          Thread.sleep(timeoutInMilliseconds);
        }
        catch (InterruptedException interruptedEx)
        {
          log.error("Thread interrupted.");
          Thread.currentThread().interrupt();
        }
      }
    }

    // Retry one last time, the exception will by handled by the caller
    return method.get();
  }

  /**
   * Helper method for simple retries. Overload for default arguments.
   * @param <T> The type of object returned by the supplier
   * @param <E> The type of exception thrown by the supplier
   * @param method The method to be executed and retried on.
   */
  private static <T, E extends Throwable> T retry(
      ThrowableSupplier<T, E> method,
      SnowflakeConf snowflakeConf)
  throws E
  {
    int maxRetries = snowflakeConf.getInt(
        SnowflakeConf.ConfVars.SNOWFLAKE_HIVEMETASTORELISTENER_RETRY_COUNT.getVarname(), 3);
    int timeoutInMilliseconds = snowflakeConf.getInt(
        SnowflakeConf.ConfVars.SNOWFLAKE_HIVEMETASTORELISTENER_RETRY_TIMEOUT_MILLISECONDS.getVarname(), 1000);
    return retry(method, maxRetries, timeoutInMilliseconds);
  }
}
