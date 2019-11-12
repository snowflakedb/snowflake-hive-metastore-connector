/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.commands;

import com.google.common.base.Preconditions;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.util.HiveToSnowflakeType;
import net.snowflake.hivemetastoreconnector.util.StageCredentialUtil;
import net.snowflake.hivemetastoreconnector.util.StringUtil;
import net.snowflake.hivemetastoreconnector.core.SnowflakeClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;

import java.lang.UnsupportedOperationException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A class for the CreateExternalTable command
 * @author xma
 */
public class CreateExternalTable extends Command
{
  /**
   * Creates a CreateExternalTable command
   * @param createTableEvent Event to generate a command from
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  public CreateExternalTable(CreateTableEvent createTableEvent,
                             SnowflakeConf snowflakeConf)
  {
    this(Preconditions.checkNotNull(createTableEvent).getTable(),
         snowflakeConf,
         createTableEvent.getHandler().getConf(),
         true);
  }

  /**
   * Creates a CreateExternalTable command, without an event
   * @param hiveTable The Hive table to generate a command from
   * @param snowflakeConf The configuration for Snowflake Hive metastore
   *                      listener
   * @param hiveConf The Hive configuration
   * @param canReplace Whether to replace existing resources or not
   */
  public CreateExternalTable(Table hiveTable,
                             SnowflakeConf snowflakeConf,
                             Configuration hiveConf,
                             boolean canReplace)
  {
    super(hiveTable);
    this.hiveTable = Preconditions.checkNotNull(hiveTable);
    this.snowflakeConf = Preconditions.checkNotNull(snowflakeConf);
    this.hiveConf = Preconditions.checkNotNull(hiveConf);
    this.canReplace = canReplace;
  }

  /**
   * Helper method to generate a stage name for newly created stages.
   * @param hiveTable The Hive table to generate a command from
   * @param snowflakeConf The configuration for Snowflake Hive metastore
   *                      listener
   * @return The generated stage name. For example, "someDb__aTable".
   */
  public static String generateStageName(Table hiveTable,
                                         SnowflakeConf snowflakeConf)
  {
    return String.format(
        "%s__%s", // double underscore
        StringUtil.escapeSqlIdentifier(snowflakeConf.get(
            SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_DB.getVarname(),
            null)),
        StringUtil.escapeSqlIdentifier(hiveTable.getTableName()));
  }

  /**
   * Helper method that generates a create stage command
   * @param canReplace Whether the stage should be replaced if one exists
   * @param stageName The name of the stage
   * @param location The URL of the stage
   * @param extraArguments A string that contains any additional arguments,
   *                       for example, "STORAGE_INTEGRATION='storageIntegration'"
   * @return The generated command. An example of the command would be:
   *         CREATE OR REPLACE STAGE s1 URL='s3://bucketname/path/to/table'
   *         STORAGE_INTEGRATION='storageIntegration'
   *         COMMENT='Generated with Hive metastore connector (version=1.2.3).';
   */
  private static String generateCreateStageCommand(boolean canReplace,
                                                   String stageName,
                                                   String location,
                                                   String extraArguments)
  {
    return String.format("CREATE %sSTAGE %s%s URL='%s'\n%s " +
                             "COMMENT='Generated with Hive metastore connector (version=%s).';",
                           (canReplace ? "OR REPLACE " : ""),
                           (canReplace ? "" : "IF NOT EXISTS "),
                           StringUtil.escapeSqlIdentifier(stageName),
                           StringUtil.escapeSqlText(location),
                           extraArguments,
                           getConnectorVersion());
  }

  /**
   * Helper method to get the version of the connector (aka the Maven
   * artifact version).
   * @return The Hive metastore connector version
   */
  private static String getConnectorVersion()
  {
    return CreateExternalTable.class.getPackage().getImplementationVersion();
  }

  /**
   * Generate the string for a column to be used in the query
   * @param columnSchema Details about the column
   * @param columnPosition Position of this column (used for CSV columns only).
   *                       Zero-based.
   * @param snowflakeFileFormatType Snowflake's file format type
   * @param snowflakeConf The configuration for Snowflake Hive metastore
   *                      listener
   * @return Snippet of a command that represents a column, for example:
   *         col1 INT as (VALUE:c1::INT)
   */
  public static String generateColumnStr(FieldSchema columnSchema,
                                         int columnPosition,
                                         HiveToSnowflakeType.SnowflakeFileFormatType snowflakeFileFormatType,
                                         SnowflakeConf snowflakeConf)
  {
    String snowflakeType = HiveToSnowflakeType
        .toSnowflakeColumnDataType(columnSchema.getType());
    StringBuilder sb = new StringBuilder();
    sb.append(StringUtil.escapeSqlIdentifier(columnSchema.getName()));
    sb.append(" ");
    sb.append(snowflakeType);
    sb.append(" as (VALUE:");
    if (snowflakeFileFormatType == HiveToSnowflakeType.SnowflakeFileFormatType.CSV)
    {
      // For CSV, Snowflake populates VALUE with the keys c1, c2, etc. for each
      // column
      sb.append("c");
      sb.append((columnPosition+1));
    }
    else
    {
      // Note: With Snowflake, keys in VALUE are case-sensitive. Rely on the
      //       user to provide columns with casing that match the data.
      String columnName = columnSchema.getName();
      String casingOverride = snowflakeConf.get(
          SnowflakeConf.ConfVars.SNOWFLAKE_DATA_COLUMN_CASING.getVarname(), "NONE");
      if (casingOverride.equalsIgnoreCase("UPPER"))
      {
        columnName = columnName.toUpperCase();
      }
      else if (casingOverride.equalsIgnoreCase("LOWER"))
      {
        columnName = columnName.toLowerCase();
      }
      sb.append(StringUtil.escapeSqlIdentifier(columnName));
    }
    sb.append("::");
    sb.append(snowflakeType);
    sb.append(')');
    return sb.toString();
  }

  /**
   * Generate the string for a partition column to be used in the query
   * @param columnSchema Details about the column
   * @return Snippet of a command that represents a partition column, e.g.
   *         partcol INT as
   *            (parse_json(metadata$external_table_partition):PARTCOL::INT)
   */
  private String generatePartitionColumnStr(FieldSchema columnSchema)
  {
    String snowflakeType = HiveToSnowflakeType
        .toSnowflakeColumnDataType(columnSchema.getType());
    StringBuilder sb = new StringBuilder();
    sb.append(StringUtil.escapeSqlIdentifier(columnSchema.getName()));
    sb.append(" ");
    sb.append(snowflakeType);
    sb.append(" as (parse_json(metadata$external_table_partition):");
    sb.append(StringUtil.escapeSqlIdentifier(columnSchema.getName().toUpperCase()));
    sb.append("::");
    sb.append(snowflakeType);
    sb.append(')');
    return sb.toString();
  }

  /**
   * Generate the create table command
   * @return The equivalent Snowflake command generated, for example:
   *         CREATE OR REPLACE EXTERNAL TABLE t1(
   *             partcol INT as
   *               (parse_json(metadata$external_table_partition):PARTCOL::INT),
   *             name STRING as
   *               (parse_json(metadata$external_table_partition):NAME::STRING))
   *           partition by (partcol,name)location=@s1
   *           partition_type=user_specified file_format=(TYPE=CSV)
   *           AUTO_REFRESH=false
   *           COMMENT='Generated with Hive metastore connector (version=1.2.3).';
   */
  private String generateCreateTableCommand(String location)
    throws UnsupportedOperationException
  {

    StringBuilder sb = new StringBuilder();

    // create table command
    sb.append(String.format("CREATE %sEXTERNAL TABLE %s",
                            (canReplace ? "OR REPLACE " : ""),
                            (canReplace ? "" : "IF NOT EXISTS ")));
    sb.append(StringUtil.escapeSqlIdentifier(hiveTable.getTableName()));

    // columns
    List<FieldSchema> cols = hiveTable.getSd().getCols();
    List<FieldSchema> partCols = hiveTable.getPartitionKeys();

    // determine the file format type for Snowflake
    HiveToSnowflakeType.SnowflakeFileFormatType sfFileFmtType =
        HiveToSnowflakeType.toSnowflakeFileFormatType(
          hiveTable.getSd().getSerdeInfo().getSerializationLib(),
          hiveTable.getSd().getInputFormat());

    if (!cols.isEmpty() || !partCols.isEmpty())
    {
      sb.append("(");

      // With Snowflake, partition columns are defined with normal columns
      for (int i = 0; i < cols.size(); i++)
      {
        sb.append(generateColumnStr(
            cols.get(i), i, sfFileFmtType, snowflakeConf));
        if (!partCols.isEmpty() || i != cols.size() - 1)
        {
          sb.append(",");
        }
      }

      for (int i = 0; i < partCols.size(); i++)
      {
        sb.append(generatePartitionColumnStr(partCols.get(i)));
        if (i != partCols.size() - 1)
        {
          sb.append(",");
        }
      }

      sb.append(")");
    }

    if (!partCols.isEmpty())
    {
      // Use user specified partitions

      // partition columns
      sb.append("partition by (");

      for (int i = 0; i < partCols.size(); ++i)
      {
        sb.append(partCols.get(i).getName());
        if (i != partCols.size() - 1)
        {
          sb.append(",");
        }
      }
      sb.append(")");

      // partition_type
      sb.append("partition_type=user_specified ");
    }
    else
    {
      // Use implicitly specified partitions
      sb.append("partition_type=implicit ");
    }


    // location
    sb.append("location=@");
    sb.append(StringUtil.escapeSqlIdentifier(location) + " ");

    // file_format
    sb.append("file_format=");
    sb.append(HiveToSnowflakeType.toSnowflakeFileFormat(
        sfFileFmtType,
        hiveTable.getSd().getSerdeInfo(),
        hiveTable.getParameters()));

    // All Hive-created tables have auto refresh disabled
    sb.append(" AUTO_REFRESH=false");

    // Add the connector version in the comments
    sb.append(" COMMENT='Generated with Hive metastore connector (version=");
    sb.append(getConnectorVersion());
    sb.append(").'");

    sb.append(";");

    return sb.toString();
  }

  /**
   * Generate the create stage command or get a location suitable for a
   * create table command.
   * @return a tuple with the Snowflake command generated, and a location
   *         suitable for a create table command. If a stage does not need to
   *         be created, the command is empty.
   *         Examples of the command include:
   *           CREATE OR REPLACE STAGE s1 URL='s3://bucketname/path/to/table'
   *             credentials=(AWS_KEY_ID='{accessKeyId}'
   *                          AWS_SECRET_KEY='{awsSecretKey}');
   *
   *           CREATE OR REPLACE STAGE s1 URL='s3://bucketname/path/to/table'
   *             STORAGE_INTEGRATION='storageIntegration';
   */
  private LocationWithCreateStageQuery generateLocationWithCommand()
      throws SQLException
  {
    String hiveTableLocation = hiveTable.getSd().getLocation();
    String integration = snowflakeConf.get(
        SnowflakeConf.ConfVars.SNOWFLAKE_INTEGRATION_FOR_HIVE_EXTERNAL_TABLES.getVarname(), null);
    String stage = snowflakeConf.get(
        SnowflakeConf.ConfVars.SNOWFLAKE_STAGE_FOR_HIVE_EXTERNAL_TABLES.getVarname(), null);

    String location;
    String command;
    if (integration != null)
    {
      // An integration was provided. Use it to create a stage
      location = generateStageName(hiveTable, snowflakeConf);
      command = generateCreateStageCommand(
          this.canReplace,
          location,
          HiveToSnowflakeType.toSnowflakeURL(hiveTableLocation),
          String.format("STORAGE_INTEGRATION=%s",
                        StringUtil.escapeSqlIdentifier(integration)));
    }
    else if (stage != null)
    {
      // A stage was specified, use it
      String tableLocation = HiveToSnowflakeType.toSnowflakeURL(hiveTableLocation);
      String stageLocation = getStageLocationFromStageName(stage);
      String relativeLocation =
          StringUtil.relativizeURI(stageLocation, tableLocation)
              .orElseThrow(() -> new IllegalArgumentException(String.format(
                  "The table location must be a subpath of the stage " +
                      "location. tableLocation: '%s', stageLocation: '%s'",
                  tableLocation,
                  stageLocation)));

      location = stage + "/" + relativeLocation;
      command = null;
    }
    else if (snowflakeConf.getBoolean(
        SnowflakeConf.ConfVars.SNOWFLAKE_ENABLE_CREDENTIALS_FROM_HIVE_CONF.getVarname(), false))
    {
      // No stage was specified, create one
      location = generateStageName(hiveTable, snowflakeConf);
      command = generateCreateStageCommand(
          this.canReplace,
          location,
          HiveToSnowflakeType.toSnowflakeURL(hiveTableLocation),
          StageCredentialUtil
              .generateCredentialsString(hiveTableLocation, hiveConf));
    }
    else
    {
      throw new IllegalArgumentException(String.format(
          "Configuration does not specify a stage to use. Add a " +
              "configuration for %s to " +
              "specify the stage.",
          SnowflakeConf.ConfVars.SNOWFLAKE_STAGE_FOR_HIVE_EXTERNAL_TABLES.getVarname()));
    }

    Preconditions.checkNotNull(location);
    return new LocationWithCreateStageQuery(location, Optional.ofNullable(command));
  }

  private String getStageLocationFromStageName(String stageName)
      throws SQLException
  {
    // Go to Snowflake to fetch the stage location. Note: Case-insensitive
    ResultSet result = SnowflakeClient.executeStatement(
        String.format("SHOW STAGES LIKE '%s';", StringUtil.escapeSqlText(stageName)),
        snowflakeConf);

    // Find a column called 'url', which contains the stage location. There
    // should be exactly one row.
    int urlPropertyIndex = -1;
    for (int i = 1; i <= result.getMetaData().getColumnCount(); i++)
    {
      if (result.getMetaData().getColumnName(i).toUpperCase().equals("URL"))
      {
        urlPropertyIndex = i;
        break;
      }
    }
    Preconditions.checkState(urlPropertyIndex != -1,
                             "Could not find URL property for stage: ", stageName);

    // Call result.next() once to get to the first row.
    Preconditions.checkState(result.next(), "Could not find stage: ", stageName);

    return Preconditions.checkNotNull(
        result.getString(urlPropertyIndex),
        "Could not find URL for stage: ", stageName);
  }

  /**
   * Generates the queries for create external table
   * The behavior of this method is as follows (in order of preference):
   *  a. If the user specifies an integration, use the integration to create
   *     a stage. Then, use the stage to create a table.
   *  b. If the user specifies a stage, use the stage to create a table.
   *  c. If the user allows this listener to read from Hive configs, use the AWS
   *     credentials from the Hive config to create a stage. Then, use the
   *     stage to create a table.
   *  d. Raise an error. Do not create a table.
   *
   * @return The Snowflake query generated
   * @throws SQLException Thrown when there was an error executing a Snowflake
   *                      SQL query (if a Snowflake query must be executed).
   * @throws UnsupportedOperationException Thrown when the input is invalid
   * @throws IllegalArgumentException Thrown when arguments are illegal
   */
  public List<String> generateSqlQueries()
      throws SQLException, UnsupportedOperationException,
             IllegalArgumentException
  {
    List<String> queryList = new ArrayList<>();

    LocationWithCreateStageQuery stageLocationAndCommand = generateLocationWithCommand();
    String location = stageLocationAndCommand.getLocation();
    stageLocationAndCommand.getQuery().ifPresent(queryList::add);

    Preconditions.checkNotNull(location);
    queryList.add(generateCreateTableCommand(location));

    if (this.hiveTable.getPartitionKeys().isEmpty())
    {
      // Refresh implicitly partitioned tables after creation
      queryList.add(String.format("ALTER EXTERNAL TABLE %s REFRESH;",
                                  StringUtil.escapeSqlIdentifier(hiveTable.getTableName())));
    }

    return queryList;
  }

  private final Table hiveTable;

  private final Configuration hiveConf;

  private final SnowflakeConf snowflakeConf;

  private boolean canReplace;

  /**
   * Class that contains output from generateLocation.
   * Contains a location suitable for creating a table with, as well as the
   * query to create its stage with, if a new stage should be used.
   */
  private class LocationWithCreateStageQuery
  {
    private String location;

    private Optional<String> query;

    LocationWithCreateStageQuery(String location, Optional<String> query)
    {
      this.location = location;
      this.query = query;
    }

    String getLocation()
    {
      return location;
    }

    Optional<String> getQuery()
    {
      return query;
    }
  }
}
