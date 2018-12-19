/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.util;

import com.google.common.collect.ImmutableMap;

/**
 * A util to convert hive types such as the hive datatype to snowflake types.
 */
public class HiveToSnowflakeType
{
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
   * TODO: Add more datatypes and include SerDe information
   * @param hiveFileFormat
   * @return
   * @throws Exception
   */
  public static String toSnowflakeFileFormat(String hiveFileFormat)
  throws Exception
  {
    if (hiveFileFormat.equals("org.apache.hadoop.mapred.TextInputFormat"))
    {
      return "(type=CSV)";
    }
    throw new Exception("Snowflake does not support the corresponding " +
                        "Hive file format: " + hiveFileFormat);
  }


}
