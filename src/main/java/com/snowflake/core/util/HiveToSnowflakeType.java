/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A util to convert hive types such as the hive datatype to snowflake types.
 */
public class HiveToSnowflakeType
{
  /**
   * The mapping from a hive datatype to a snowflake datatype
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
   * converts a hive column data type to a snowflake datatype
   * @param hiveType
   * @return
   * @throws Exception
   */
  public static String toSnowflakeColumnDataType(String hiveType)
  throws Exception
  {
    if (hiveToSnowflakeDataTypeMap.containsKey(hiveType.toUpperCase()))
    {
      return hiveToSnowflakeDataTypeMap.get(hiveType.toUpperCase());
    }
    throw new Exception("Snowflake does not support the corresponding " +
                        "Hive data type: " + hiveType);
  }

  /**
   * converts a hive url to a snowflake url
   * @param hiveUrl
   * @return
   * @throws Exception
   */
  public static String toSnowflakeURL(String hiveUrl)
      throws Exception
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
    throw new Exception("Snowflake does not support the external location");
  }

  /**
   * converts a hive file format to a snowflake file format
   * @param hiveFileFormat
   * @param serDeInfo
   * @param tableProps
   * @return
   * @throws Exception
   */
  public static String toSnowflakeFileFormat(String hiveFileFormat,
                                             SerDeInfo serDeInfo,
                                             Map<String, String> tableProps)
  throws Exception
  {
    Map<String, String> snowflakeFileFormatOptions = new HashMap<>();
    Map<String, String> serDeParams = serDeInfo.getParameters();

    String sfFileFmtType = toSnowflakeFileFormatType(
        serDeInfo.getSerializationLib(), hiveFileFormat);
    snowflakeFileFormatOptions.put("TYPE", sfFileFmtType);

    // Each Snowflake file format type has its own set of options. Attempt to
    // infer these from the SerDe parameters and table properties.
    switch (sfFileFmtType)
    {
      case "CSV":
        String fieldDelimiter = serDeParams.getOrDefault("field.delim", null);
        if (fieldDelimiter != null)
        {
          snowflakeFileFormatOptions.put("FIELD_DELIMITER", fieldDelimiter);
        }

        String lineDelimiter = serDeParams.getOrDefault("line.delim", null);
        if (lineDelimiter != null)
        {
          snowflakeFileFormatOptions.put("RECORD_DELIMITER", lineDelimiter);
        }

        String escape = serDeParams.getOrDefault("escape.delim", null);
        if (escape != null)
        {
          snowflakeFileFormatOptions.put("ESCAPE", escape);
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
              throw new Exception("Snowflake does not support the following" +
                                      "compression format for Parquet: " + compression);
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
   * @param serDeLib
   * @param hiveFileFormat
   * @return
   * @throws Exception
   */
  private static String toSnowflakeFileFormatType(String serDeLib,
                                                  String hiveFileFormat)
  throws Exception
  {
    // If a Snowflake file format type is a substring of the SerDe, assume that
    // Snowflake file format is appropriate. For example:
    //   org.apache.hive.hcatalog.data.JsonSerDe -> JSON
    //   org.apache.hadoop.hive.serde2.JsonSerDe -> JSON
    //   org.apache.hadoop.hive.serde2.OpenCSVSerde -> CSV
    for (String sfFileFmtType : snowflakeFileFormatTypes)
    {
      // Assume sfFileFmtType is uppercase
      if (serDeLib.toUpperCase().contains(sfFileFmtType))
      {
        return sfFileFmtType;
      }
    }

    // For textfiles types with SerDe's like LazySimpleSerDe, fall back to CSV
    if (hiveFileFormat.equals("org.apache.hadoop.mapred.TextInputFormat"))
    {
      return "CSV";
    }

    throw new Exception("Snowflake does not support the corresponding " +
                        "SerDe: " + serDeLib);
  }
}
