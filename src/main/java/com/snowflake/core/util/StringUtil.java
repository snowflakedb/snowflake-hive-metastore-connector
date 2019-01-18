package com.snowflake.core.util;

import javafx.util.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    if (args.isEmpty())
    {
      return format;
    }

    // Create a regex that matches a set of strings, e.g. {(one|two|three)}
    Pattern keyPattern = Pattern.compile(
      "\\{(" +
        String.join("|", args.keySet()) +
      ")\\}");
    Matcher matcher = keyPattern.matcher(format);

    // Find each match, and replace then the appropriate value.
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      String found = matcher.group(1);
      matcher.appendReplacement(sb, args.get(found));
    }
    matcher.appendTail(sb);

    return sb.toString();
  }

  /**
   * A utility class for strings that cannot be logged directly due to
   * potentially sensitive contents.
   */
  public static class SensitiveString extends Pair<String, Map<String, String>>
  {
    /**
     * Constructor for a sensitive string with no sensitive information
     * @param string
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
    public Map<String, String> getSensitiveValues()
    {
      return this.getValue();
    }
  }
}