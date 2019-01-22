/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A util to convert Hive types such as the hive datatype to Snowflake types.
 */
public class HiveToSnowflakeType
{
  /**
   * The mapping from a Hive datatype to a Snowflake datatype
   */
  public static ImmutableMap<String, String> hiveToSnowflakeDataTypeMap =
      new ImmutableMap.Builder<String, String>()
      .put("BOOLEAN", "BOOLEAN")
      .put("TINYINT", "SMALLINT")
      .put("SMALLINT", "SMALLINT")
      .put("INT", "INT")
      .put("BIGINT", "BIGINT")
      .put("FLOAT", "FLOAT")
      .put("DOUBLE", "DOUBLE")
      .put("STRING", "STRING")
      .put("CHAR", "CHAR")
      .put("VARCHAR", "VARCHAR")
      .put("DATE", "DATE")
      .put("TIMESTAMP", "TIMESTAMP")
      .put("BINARY", "BINARY")
      .put("DECIMAL", "DECIMAL")
      .build();

  /**
   * List of file format types suppported by Snowflake
   */
  public static ImmutableList<String> snowflakeFileFormatTypes =
      new ImmutableList.Builder<String>()
      .add("CSV")
      .add("JSON")
      .add("AVRO")
      .add("ORC")
      .add("PARQUET")
      .add("XML")
      .build();

  /**
   * Regex pattern to match a set of strings, e.g. (CSV|JSON|PARQUET)
   */
  private static Pattern sfFileFmtTypeRegex = Pattern.compile(
      "(" + String.join("|", snowflakeFileFormatTypes) + ")");

  /**
   * converts a Hive column data type to a Snowflake datatype
   * @param hiveType The data type of the column according to Hive
   * @return The corresponding Snowflake data type
   * @throws IllegalArgumentException Thrown when the data type is invalid or
   *                                  unsupported.
   */
  public static String toSnowflakeColumnDataType(String hiveType)
      throws IllegalArgumentException
  {
    if (hiveToSnowflakeDataTypeMap.containsKey(hiveType.toUpperCase()))
    {
      return hiveToSnowflakeDataTypeMap.get(hiveType.toUpperCase());
    }
    throw new IllegalArgumentException(
        "Snowflake does not support the corresponding Hive data type: " +
            hiveType);
  }

  /**
   * converts a Hive URL to a Snowflake URL
   * @param hiveUrl The Hive URL
   * @return The URL as understood by Snowflake
   * @throws IllegalArgumentException Thrown when the input is invalid
   */
  public static String toSnowflakeURL(String hiveUrl)
      throws IllegalArgumentException
  {
    String snowflakeUrl;
    // for now, only handle stages on aws
    // hive can have prefixes 's3n' or 's3a', do some processing for Snowflake
    if (hiveUrl.startsWith("s3"))
    {
      int colonIndex = hiveUrl.indexOf(":");
      snowflakeUrl = hiveUrl.substring(0, 2) + hiveUrl.substring(colonIndex);
      return snowflakeUrl;
    }
    throw new IllegalArgumentException(
        "Snowflake does not support the external location");
  }

  /**
   * converts a Hive file format to a Snowflake file format
   * @param sfFileFmtType Snowflake's file format type
   * @param serDeInfo Details about the SerDe
   * @param tableProps Table properties the table was created with
   * @return Snippet representing a Snowflake file format
   * @throws IllegalArgumentException Thrown when the input is invalid
   */
  public static String toSnowflakeFileFormat(String sfFileFmtType,
                                             SerDeInfo serDeInfo,
                                             Map<String, String> tableProps)
      throws IllegalArgumentException
  {
    Map<String, String> snowflakeFileFormatOptions = new HashMap<>();
    Map<String, String> serDeParams = serDeInfo.getParameters();
    snowflakeFileFormatOptions.put("TYPE", sfFileFmtType);

    // Each Snowflake file format type has its own set of options. Attempt to
    // infer these from the SerDe parameters and table properties.
    switch (sfFileFmtType)
    {
      case "CSV":
        String fieldDelimiter = serDeParams.getOrDefault("field.delim", null);
        if (fieldDelimiter != null)
        {
          snowflakeFileFormatOptions.put("FIELD_DELIMITER",
                                         String.format("'%s'", fieldDelimiter));
        }

        String lineDelimiter = serDeParams.getOrDefault("line.delim", null);
        if (lineDelimiter != null)
        {
          snowflakeFileFormatOptions.put("RECORD_DELIMITER",
                                         String.format("'%s'", lineDelimiter));
        }

        String escape = serDeParams.getOrDefault("escape.delim", null);
        if (escape != null)
        {
          snowflakeFileFormatOptions.put("ESCAPE",
                                         String.format("'%s'", escape));
        }
        break;
      case "PARQUET":
        String compression = tableProps.getOrDefault("parquet.compression",null);
        if (compression != null)
        {
          // Snowflake only supports snappy compression for parquet files
          switch (compression.toUpperCase())
          {
            case "SNAPPY":
              snowflakeFileFormatOptions.put("SNAPPY_COMPRESSION", "TRUE");
              break;
            case "NONE":
            case "UNCOMPRESSED":
              snowflakeFileFormatOptions.put("SNAPPY_COMPRESSION", "FALSE");
              break;
            default:
              throw new IllegalArgumentException(
                  "Snowflake does not support the following compression " +
                      "format for Parquet: " + compression);
          }
        }
    }

    // Convert the file format options map to something like
    // "(TYPE=CSV,FIELD_DELIMITER='|')"
    String optionsAsString = snowflakeFileFormatOptions.entrySet()
      .stream()
      .map(opt -> String.format("%1$s=%2$s", opt.getKey(), opt.getValue()))
      .collect(Collectors.joining(","));
    return String.format("(%s)", optionsAsString);
  }

  /**
   * Determines the most appropriate Snowflake file format type for a given Hive
   * file format and SerDe.
   * @param serDeLib The SerDe class that the table was created with
   * @param hiveFileFormat The file format according to Hive
   * @return The corresponding Snowflake file format type
   * @throws IllegalArgumentException Thrown when the SerDe is invalid or
   *                                  unsupported.
   */
  public static String toSnowflakeFileFormatType(String serDeLib,
                                                 String hiveFileFormat)
      throws IllegalArgumentException
  {
    // If a Snowflake file format type is a substring of the SerDe, assume that
    // Snowflake file format is appropriate. For example:
    //   org.apache.hive.hcatalog.data.JsonSerDe -> JSON
    //   org.apache.hadoop.hive.serde2.JsonSerDe -> JSON
    //   org.apache.hadoop.hive.serde2.OpenCSVSerde -> CSV

    Matcher matcher = sfFileFmtTypeRegex.matcher(serDeLib.toUpperCase());
    if (matcher.find()) {
      return matcher.group(1);
    }

    // For textfiles types with SerDe's like LazySimpleSerDe, fall back to CSV
    if (hiveFileFormat.equals("org.apache.hadoop.mapred.TextInputFormat"))
    {
      return "CSV";
    }

    throw new IllegalArgumentException(
        "Snowflake does not support the corresponding SerDe: " + serDeLib);
  }
}
