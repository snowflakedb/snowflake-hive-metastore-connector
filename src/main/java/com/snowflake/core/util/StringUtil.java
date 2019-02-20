package com.snowflake.core.util;

import com.google.common.base.Preconditions;
import javafx.util.Pair;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * A utility class for formatting strings
 */
public class StringUtil
{
  /**
   * Formats a string from a mapping.
   * For example, the string "{one} + {two} = {three}" formatted with the map
   * {"one":"1", "two":"2", "three":"3"} should yield the string "1 + 2 = 3".
   * @param format The string to format, with substrings to be replaced in the
   *               form {key}.
   * @param args Map that represents which strings should be replaced with what
   *             keys. Keys should by non-empty and consist of alphanumeric
   *             characters.
   * @return The formatted string.
   */
  public static String format(String format, Map<String, String> args)
  {
    return StringSubstitutor.replace(format, args, "{", "}");
  }

  /**
   * A utility class for strings that cannot be logged directly due to
   * potentially sensitive contents.
   */
  public static class SensitiveString extends Pair<String, Map<String, String>>
  {
    /**
     * Constructor for a sensitive string with no sensitive information
     * @param string The string
     * @see com.snowflake.core.util.StringUtil#format
     */
    public SensitiveString(String string)
    {
      super(string, new HashMap<>());
    }

    /**
     * Constructor for a sensitive string
     * @param format The string with the sensitive contents replaced with
     *               placeholders.
     * @param sensitiveValues The actual values of sensitive content as
     *                        referenced by the formatted string.
     * @see com.snowflake.core.util.StringUtil#format
     */
    public SensitiveString(String format,
                           Map<String, String> sensitiveValues)
    {
      super(format, sensitiveValues);
    }

    @Override
    public String toString()
    {
      return this.getKey();
    }

    /**
     * Method that generates the content of the string *with* sensitive content
     * @return The string with sensitive content.
     */
    public String toStringWithSensitiveValues()
    {
      return StringUtil.format(this.getKey(), this.getValue());
    }

    /**
     * Method that outputs the sensitive information of this sensitive string
     * @return A map representing sensitive values and what they are referred
     *         as in the formatted string.
     */
    public Map<String, String> getSecrets()
    {
      return this.getValue();
    }
  }

  /**
   * Helper method for getting a relative path between a Hive table and
   * Hive partition. The partition location must be a subpath of the table
   * location.
   * @param hiveTable The table to get a relative path from.
   * @param hivePartition The partition with a location relative to the table
   * @return The relative path between the table and partition.
   * @throws IllegalArgumentException
   */
  public static String relativizePartitionURI(Table hiveTable,
                                              Partition hivePartition)
      throws IllegalArgumentException
  {
    // For partitions, Hive requires absolute paths, while Snowflake requires
    // relative paths.
    URI tableLocation = URI.create(hiveTable.getSd().getLocation());
    URI partitionLocation = URI.create(hivePartition.getSd().getLocation());
    URI relativeLocation = tableLocation.relativize(partitionLocation);

    // If the relativized URI is still absolute, then relativizing failed
    // because the partition location was invalid.
    if (relativeLocation.isAbsolute())
    {
      throw new IllegalArgumentException(String.format(
          "The partition location must be a subpath of the table location. " +
              "Table location: '%s' Partition location: '%s'",
          tableLocation,
          partitionLocation));
    }

    return relativeLocation.toString();
  }
}
