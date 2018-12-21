/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import java.util.List;

/**
 * The command interface for external table commands
 * @author xma
 */
public interface Command
{
  /**
   * Generates the query in a string form to be sent to GS
   */
  List<String> generateCommands() throws Exception;
}
