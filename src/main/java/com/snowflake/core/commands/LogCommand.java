/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.snowflake.core.util.StringUtil.SensitiveString;

import java.util.List;

/**
 * A class for no-op commands used for logging purposes, as queries sent to
 * Snowflake will be logged.
 */
public class LogCommand implements Command
{
  /**
   * Constructor for LogCommand
   * @param log A string to be logged
   */
  public LogCommand(String log)
  {
    this.log = Preconditions.checkNotNull(log);
  }

  /**
   * An overload of the constructor to log errors
   * @param error An exception to be logged as an error
   */
  public LogCommand(Exception error)
  {
    Preconditions.checkNotNull(error);
    this.log = String.format("HIVE METASTORE LISTENER ERROR: '%s'",
                             error.getMessage());
  }

  /**
   * Generates no-op logging commands, for example:
   * SELECT NULL \* LOGS IN COMMENTS *\;
   * @return The Snowflake commands generated
   */
  public List<SensitiveString> generateCommands()
  {
    return ImmutableList.<SensitiveString>builder()
        .add(new SensitiveString(
            String.format("SELECT NULL \\* %s *\\;", this.log)
                .replace("*\\", "* \\")))
        .build().asList();
  }

  private final String log;
}
