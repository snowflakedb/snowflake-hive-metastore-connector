/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.snowflake.core.util.HiveToSnowflakeType;
import com.snowflake.core.util.HiveToSnowflakeType.SnowflakeFileFormatTypes;
import com.snowflake.core.util.StageCredentialUtil;
import com.snowflake.core.util.StringUtil.SensitiveString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;

import javax.transaction.NotSupportedException;
import java.util.ArrayList;
import java.util.List;

/**
 * A class for the CreateExternalTable command
 * @author xma
 */
public class CreateExternalTable implements Command
{
  /**
   * Creates a CreateExternalTable command
   * @param createTableEvent Event to generate a command from
   */
  public CreateExternalTable(CreateTableEvent createTableEvent)
  {
    Preconditions.checkNotNull(createTableEvent);
    this.hiveTable = Preconditions.checkNotNull(createTableEvent.getTable());
    this.hiveConf = Preconditions.checkNotNull(
        createTableEvent.getHandler().getConf());
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
  throws Exception
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
  private String generateCreateTableCommand()
  throws Exception
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
    sb.append(hiveTable.getTableName() + " ");

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

  /**
   * Generates the commands for create external table
   * Generates a create stage command followed by a create
   * external table command
   * @return The Snowflake commands generated
   * @throws Exception Thrown when the input is invalid
   */
  public List<SensitiveString> generateCommands()
      throws Exception
  {
    List<SensitiveString> queryList = new ArrayList<>();

    SensitiveString createStageQuery = generateCreateStageCommand();

    queryList.add(createStageQuery);

    String createTableQuery = generateCreateTableCommand();

    queryList.add(new SensitiveString(createTableQuery));

    return queryList;
  }

  private Table hiveTable;

  private Configuration hiveConf;
}
