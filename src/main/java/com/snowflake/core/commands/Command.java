/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.snowflake.core.util.StringUtil.SensitiveString;

import java.util.List;

/**
 * The command interface for external table commands
 * @author xma
 */
public interface Command
{
  /**
   * Generates the query in a string form to be sent to Snowflake
   * @return The Snowflake commands generated
   */
  List<SensitiveString> generateCommands() throws Exception;
}
