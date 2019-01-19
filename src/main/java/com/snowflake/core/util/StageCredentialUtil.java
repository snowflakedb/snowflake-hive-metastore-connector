/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.util;

import com.snowflake.core.util.StringUtil.SensitiveString;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * A utility function to help with retrieving stage credentials and generating
 * the credential string for Snowflake.
 */
public enum StageCredentialUtil
{
  // TODO: Support more stages
  AWS;

  /**
   * Get a stage type from the Hive table location
   * @param url The URL
   * @return Snowflake's corresponding stage type
   * @throws Exception Thrown when the input is invalid
   */
  private static StageCredentialUtil getStageTypeFromURL(String url)
  throws Exception
  {
    if (url.startsWith("s3"))
    {
      return StageCredentialUtil.AWS;
    }
    throw new Exception("The stage type does not exist");
  }

  /**
   * Get the prefix of the location from the hiveUrl
   * Used for retrieving keys from the config.
   * @param hiveUrl The URL
   * @return The prefix/protocol from the URL
   */
  private static String getLocationPrefix(String hiveUrl)
  {
    int colonIndex = hiveUrl.indexOf(":");
    return hiveUrl.substring(0, colonIndex);
  }

  /**
   * Get the credentials for the given stage in the url.
   * @param url The URL
   * @param config The Hadoop configuration
   * @return Snippet that represents credentials for the given location
   * @throws Exception Thrown when the input is invalid
   */
  public static SensitiveString generateCredentialsString(
      String url, Configuration config)
  throws Exception
  {
    StageCredentialUtil stageType = getStageTypeFromURL(url);

    switch(stageType)
    {
      case AWS:
      {
        // get the access keys
        String prefix = getLocationPrefix(url);
        String accessKey = config.get("fs." + prefix + ".awsAccessKeyId");
        if (accessKey == null)
        {
          accessKey = config.get("fs." + prefix + ".access.key");
        }

        String secretKey = config.get("fs." + prefix + ".awsSecretAccessKey");
        if (secretKey == null)
        {
          secretKey = config.get("fs." + prefix + ".secret.key");
        }

        StringBuilder sb = new StringBuilder();
        sb.append("credentials=(");
        sb.append("AWS_KEY_ID=");
        sb.append("'" + accessKey + "'\n");
        sb.append("AWS_SECRET_KEY='{awsSecretKey}')");

        Map<String, String> secrets = new HashMap<>();
        secrets.put("awsSecretKey", secretKey);
        return new SensitiveString(sb.toString(), secrets);
      }
      default:
        throw new Exception("Cannot get credentials for the stage type: " +
            stageType.name());
    }
  }

}
