/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.PrintWriter;
import java.io.StringWriter;
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
    this.log = String.format("HIVE METASTORE LISTENER ERROR (%s): '%s'\n" +
                                 "STACKTRACE: '%s'",
                             error.getClass().getCanonicalName(),
                             error.getMessage(),
                             getStackTrace(error));
  }

  /**
   * Generates no-op logging commands, for example:
   * SELECT NULL /* LOGS IN COMMENTS * /;
   * @return The Snowflake commands generated
   */
  public List<String> generateCommands()
  {
    return ImmutableList.<String>builder()
        .add(String.format("SELECT NULL /* %s */;",
                          this.log.replace("*/", "* /")))
        .build().asList();
  }

  /**
   * Helper method to get a stack trace from an exception
   * @param ex The exception
   * @return The stack trace of the given exception
   */
  private static String getStackTrace(Exception ex)
  {
    StringWriter stringWriter = new StringWriter();
    ex.printStackTrace(new PrintWriter(stringWriter, true));
    return stringWriter.getBuffer().toString();
  }
  
  private final String log;
}
