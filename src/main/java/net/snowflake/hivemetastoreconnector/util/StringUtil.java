/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.util;

import java.net.URI;
import java.util.Optional;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * A utility class for formatting strings
 */
public class StringUtil
{
  /**
   * Helper method for getting a relative path between a Hive table and
   * Hive partition. The partition location must be a subpath of the table
   * location.
   * @param hiveTable The table to get a relative path from.
   * @param hivePartition The partition with a location relative to the table
   * @return The relative path between the table and partition.
   * @throws IllegalArgumentException Thrown if bad arguments were provided,
   *                                  e.g. if the partition is not a relative
   *                                  path of the table.
   */
  public static String relativizePartitionURI(Table hiveTable,
                                              Partition hivePartition)
      throws IllegalArgumentException
  {
    // For partitions, Hive requires absolute paths, while Snowflake requires
    // relative paths.
    Optional<String> relativePath = relativizeURI(
      hiveTable.getSd().getLocation(),
      hivePartition.getSd().getLocation());

    return relativePath.orElseThrow(
        () -> new IllegalArgumentException(String.format(
          "The partition location must be a subpath of the table location. " +
              "Table location: '%s' Partition location: '%s'",
          hiveTable.getSd().getLocation(),
          hivePartition.getSd().getLocation())));
  }

  /**
   * Helper method for getting a relative path between two URIs.
   * @param base The URI that the output path will be relative to
   * @param extended The URI that contains the relative path
   * @return The relative path if possible.
   */
  public static Optional<String> relativizeURI(String base, String extended)
  {
    URI baseUri = URI.create(base);
    URI extendedUri = URI.create(extended);
    URI relativeUri = baseUri.relativize(extendedUri);

    // If the relativized URI is still absolute, then relativizing failed
    if (relativeUri.isAbsolute())
    {
      return Optional.empty();
    }

    return Optional.of(relativeUri.toString());
  }

  /**
   * Helper method to escape SQL text
   * @param str the string to escape
   * @return the escaped string
   */
  public static String escapeSqlText(String str)
  {
    return StringEscapeUtils.escapeJava(str.replace("'", "''"));
  }

  /**
   * Helper method to escape SQL text that's an identifier
   * @param str the string to escape
   * @return the escaped string
   */
  public static String escapeSqlIdentifier(String str)
  {
    return StringEscapeUtils.escapeJava(str.replace(" ", ""));
  }

  /**
   * Helper method to escape text in a SQL comment
   * @param str the string to escape
   * @return the escaped string
   */
  public static String escapeSqlComment(String str)
  {
    return str.replace("*/", "* /");
  }

  /**
   * Helper method to escape text that's in a data type spec
   * @param str the string to escape
   * @return the escaped string
   */
  public static String escapeSqlDataTypeSpec(String str)
  {
    return str.replace(")", "").replace("(", "");
  }
}
