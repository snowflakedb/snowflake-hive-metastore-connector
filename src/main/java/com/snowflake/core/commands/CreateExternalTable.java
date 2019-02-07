/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.conf.SnowflakeConf.ConfVars;
import com.snowflake.core.util.HiveToSnowflakeType;
import com.snowflake.core.util.HiveToSnowflakeType.SnowflakeFileFormatTypes;
import com.snowflake.core.util.StageCredentialUtil;
import com.snowflake.core.util.StringUtil.SensitiveString;
import com.snowflake.jdbc.client.SnowflakeClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;

import javax.transaction.NotSupportedException;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class for the CreateExternalTable command
 * @author xma
 */
public class CreateExternalTable implements Command
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
    Preconditions.checkNotNull(createTableEvent);
    this.hiveTable = Preconditions.checkNotNull(createTableEvent.getTable());
    this.hiveConf = Preconditions.checkNotNull(
        createTableEvent.getHandler().getConf());
    this.snowflakeConf = Preconditions.checkNotNull(snowflakeConf);
  }

  /**
   * Generate the create stage command
   * @return the Snowflake command generated, for example:
   *         CREATE STAGE s1 url='s3://bucketname/path/to/table'
   *         credentials=(AWS_KEY_ID='{accessKeyId}'
   *                      AWS_SECRET_KEY='{awsSecretKey}');
   * @throws Exception Thrown when the input is invalid
   */
  private SensitiveString generateCreateStageCommand()
  throws NotSupportedException
  {
    StringBuilder sb = new StringBuilder();
    String url = hiveTable.getSd().getLocation();

    // create stage command
    sb.append("CREATE STAGE ");
    // we use the table name as the stage name since every external table must
    // be linked to a stage and every table has a unique name
    sb.append(hiveTable.getTableName() + " ");

    sb.append("url='");
    sb.append(HiveToSnowflakeType.toSnowflakeURL(url) + "'\n");

    SensitiveString credentials = StageCredentialUtil
        .generateCredentialsString(url, hiveConf);
    sb.append(credentials);
    sb.append(";");

    return new SensitiveString(sb.toString(), credentials.getSecrets());
  }

  /**
   * Generate the string for a column to be used in the query
   * @param columnSchema Details about the column
   * @param columnPosition Position of this column (used for CSV columns only)
   * @param snowflakeFileFormatType Snowflake's file format type
   * @return Snippet of a command that represents a column, for example:
   *         col1 INT as (VALUE:c1::INT)
   * @throws NotSupportedException Thrown when the column type is invalid or
   *                               unsupported.
   */
  private String generateColumnStr(FieldSchema columnSchema, int columnPosition,
                                   String snowflakeFileFormatType)
    throws NotSupportedException
  {
    String snowflakeType = HiveToSnowflakeType
        .toSnowflakeColumnDataType(columnSchema.getType());
    StringBuilder sb = new StringBuilder();
    sb.append(columnSchema.getName());
    sb.append(" ");
    sb.append(snowflakeType);
    sb.append(" as (VALUE:");
    if (snowflakeFileFormatType.equals("CSV"))
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
      sb.append(columnSchema.getName());
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
   * @throws NotSupportedException Thrown when the column type is invalid or
   *                               unsupported.
   */
  private String generatePartitionColumnStr(FieldSchema columnSchema)
    throws NotSupportedException
  {
    String snowflakeType = HiveToSnowflakeType
        .toSnowflakeColumnDataType(columnSchema.getType());
    StringBuilder sb = new StringBuilder();
    sb.append(columnSchema.getName());
    sb.append(" ");
    sb.append(snowflakeType);
    sb.append(" as (parse_json(metadata$external_table_partition):");
    sb.append(columnSchema.getName().toUpperCase());
    sb.append("::");
    sb.append(snowflakeType);
    sb.append(')');
    return sb.toString();
  }

  /**
   * Generate the create table command
   * @return The equivalent Snowflake command generated, for example:
   *         CREATE EXTERNAL TABLE t1(
   *             partcol INT as
   *               (parse_json(metadata$external_table_partition):PARTCOL::INT),
   *             name STRING as
   *               (parse_json(metadata$external_table_partition):NAME::STRING))
   *           partition by (partcol,name)location=@s1
   *           partition_type=user_specified file_format=(TYPE=CSV);
   */
  private String generateCreateTableCommand(String location)
  throws NotSupportedException
  {

    StringBuilder sb = new StringBuilder();

    // create table command
    sb.append("CREATE EXTERNAL TABLE ");
    sb.append(hiveTable.getTableName());
    sb.append("(");

    // columns
    List<FieldSchema> cols = hiveTable.getSd().getCols();
    List<FieldSchema> partCols = hiveTable.getPartitionKeys();

    // determine the file format type for Snowflake
    SnowflakeFileFormatTypes sfFileFmtType =
        HiveToSnowflakeType.toSnowflakeFileFormatType(
          hiveTable.getSd().getSerdeInfo().getSerializationLib(),
          hiveTable.getSd().getInputFormat());

    // With Snowflake, partition columns are defined with normal columns
    for (int i = 0; i < cols.size(); i++)
    {
      sb.append(generateColumnStr(cols.get(i), i, sfFileFmtType.toString()));
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

    // location
    sb.append("location=@");
    sb.append(location + " ");

    // partition_type
    sb.append("partition_type=user_specified ");

    // file_format
    sb.append("file_format=");
    sb.append(HiveToSnowflakeType.toSnowflakeFileFormat(
        sfFileFmtType,
        hiveTable.getSd().getSerdeInfo(),
        hiveTable.getParameters()));
    sb.append(";");

    return sb.toString();
  }

  private String getStageLocationFromStageName(String stageName)
      throws SQLException
  {
    // Go to Snowflake to fetch the stage location
    ResultSet result = SnowflakeClient.executeStatement(
        String.format("DESC STAGE %s;", stageName), snowflakeConf);

    // Find columns called 'property' and 'property_value', and find a row with
    // property as "URL". The property_value will contain the URL
    HashMap<String, Integer> propertyIndices = new HashMap<>();
    for (int i = 1; i <= result.getMetaData().getColumnCount(); i++)
    {
      propertyIndices.put(result.getMetaData().getColumnName(i).toUpperCase(), i);
    }
    Preconditions.checkState(propertyIndices.containsKey("PROPERTY") &&
                                 propertyIndices.containsKey("PROPERTY_VALUE"),
                             "Could not find URL property for stage: ", stageName);

    String url = null;
    while (result.next())
    {
      String property = result.getString(propertyIndices.get("PROPERTY"));
      if (property.equalsIgnoreCase("URL"))
      {
        url = result.getString(propertyIndices.get("PROPERTY_VALUE"));
        break;
      }
    }
    Preconditions.checkNotNull(url, "Could not find URL for stage: ", stageName);

    // The URL may be a list in the form '["url1", ...]'. Get the first URL
    // if it is in this form.
    Matcher matcher = Pattern.compile("\\[\"([^\"]+)\"").matcher(url);
    if (matcher.find())
    {
      url = matcher.group(1);
    }

    return url;
  }

  /**
   * Generates the commands for create external table
   * Generates a create stage command followed by a create
   * external table command
   * @return The Snowflake commands generated
   * @throws NotSupportedException Thrown when the input is invalid
   */
  public List<SensitiveString> generateCommands()
      throws NotSupportedException, SQLException
  {
    List<SensitiveString> queryList = new ArrayList<>();

    String stage = snowflakeConf.get(
        ConfVars.SNOWFLAKE_HIVEMETASTORELISTENER_STAGE.getVarname(), null);
    String location;
    if (stage != null)
    {
      // A stage was specified, use it
      URI tableLocation = URI.create(HiveToSnowflakeType.toSnowflakeURL(
          hiveTable.getSd().getLocation()));
      URI stageLocation = URI.create(getStageLocationFromStageName(stage));
      URI relativeLocation = stageLocation.relativize(tableLocation);

      // If the relativized URI is still absolute, then relativizing failed
      // because the partition location was invalid.
      Preconditions.checkArgument(
          !relativeLocation.isAbsolute(),
          String.format("The table location must be a subpath of the stage " +
                        "location. tableLocation: '%s', stageLocation: '%s', " +
                        "relativePath: '%s'", tableLocation,
                        stageLocation, relativeLocation));

      location = stage + "/" + relativeLocation;
    }
    else
    {
      // No stage was specified, create one
      SensitiveString createStageQuery = generateCreateStageCommand();
      queryList.add(createStageQuery);
      location = hiveTable.getTableName(); // Stage and table names are the same
    }

    Preconditions.checkNotNull(location);
    queryList.add(new SensitiveString(generateCreateTableCommand(location)));

    return queryList;
  }

  private final Table hiveTable;

  private final Configuration hiveConf;

  private final SnowflakeConf snowflakeConf;
}
