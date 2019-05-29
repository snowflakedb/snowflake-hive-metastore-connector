/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.jdbc.client;

import com.google.common.base.Preconditions;
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.core.commands.Command;
import com.snowflake.core.util.CommandGenerator;
import com.snowflake.core.util.Scheduler;
import com.snowflake.hive.listener.SnowflakeHiveListener;
import net.snowflake.client.jdbc.internal.apache.commons.codec.binary.Base64;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
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

  private static Scheduler scheduler;

  /**
   * Creates and executes an event for snowflake. Events may be processed in
   * the background, but events on the same table will be processed in order.
   * @param event - the hive event details
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   */
  public static void createAndExecuteCommandForSnowflake(
      ListenerEvent event,
      SnowflakeConf snowflakeConf)
  {
    Preconditions.checkNotNull(event);

    // Obtains the proper command
    log.info("Creating the Snowflake command");
    Command command = CommandGenerator.getCommand(event, snowflakeConf);

    boolean backgroundTaskEnabled = !snowflakeConf.getBoolean(
        SnowflakeConf.ConfVars.SNOWFLAKE_CLIENT_FORCE_SYNCHRONOUS.getVarname(), false);
    if (backgroundTaskEnabled)
    {
      initScheduler(snowflakeConf);
      scheduler.enqueueMessage(command);
    }
    else
    {
      generateAndExecuteSnowflakeStatements(command, snowflakeConf);
    }
  }

  /**
   * Helper method. Generates commands for an event and executes those commands.
   * Synchronous.
   * @param command - the command to generate statements from
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   */
  public static void generateAndExecuteSnowflakeStatements(
      Command command,
      SnowflakeConf snowflakeConf)
  {
    // Generate the string queries for the command
    // Some Hive commands require more than one statement in Snowflake
    // For example, for create table, a stage must be created before the table
    log.info("Generating Snowflake queries");
    List<String> commandList;
    try
    {
      commandList = command.generateSqlQueries();
      executeStatements(commandList, snowflakeConf);
    }
    catch (Exception e)
    {
      log.error("Could not generate the Snowflake commands: " + e.getMessage());
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

    scheduler = new Scheduler(numThreads, snowflakeConf);
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
        if (!confVar.isSnowflakeJDBCProperty())
        {
          return;
        }

        properties.put(confVar.getSnowflakePropertyName(), conf.getValue());
      });

    String connectStr = snowflakeConf.get(
        SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_CONNECTION.getVarname());

    String privateKeyConf = snowflakeConf.get(
        SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_PRIVATE_KEY.getVarname());

    if (privateKeyConf != null)
    {
      try
      {
        Security.addProvider(new BouncyCastleProvider());
        byte[] keyBytes = Base64.decodeBase64(privateKeyConf);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
        properties.put("privateKey", keyFactory.generatePrivate(keySpec));
      }
      catch (InvalidKeySpecException | NoSuchAlgorithmException e)
      {
        throw new IllegalArgumentException(
            String.format("Private key is invalid: %s", e), e);
      }
    }

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
