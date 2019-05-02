/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.snowflake.core.util.StringUtil;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

/**
 * A class for no-op commands used for logging purposes, as queries sent to
 * Snowflake will be logged.
 */
public class LogCommand extends Command
{
  /**
   * Constructor for LogCommand
   * @param log A string to be logged
   */
  public LogCommand(Table hiveTable, String log)
  {
    super(hiveTable);
    this.log = Preconditions.checkNotNull(log);
  }

  /**
   * An overload of the constructor to log errors
   * @param error An exception to be logged as an error
   */
  public LogCommand(Table hiveTable, Exception error)
  {
    this(hiveTable, String.format(
        "HIVE METASTORE LISTENER ERROR (%s): '%s'\nSTACKTRACE: '%s'",
        Preconditions.checkNotNull(error).getClass().getCanonicalName(),
        error.getMessage(),
        getStackTrace(error)));
  }

  /**
   * Generates no-op logging commands, for example:
   * SELECT NULL /* LOGS IN COMMENTS * /;
   * @return The Snowflake commands generated
   */
  public List<String> generateStatements()
  {
    return ImmutableList.<String>builder()
        .add(String.format("SELECT NULL /* %s */;",
                           StringUtil.escapeSqlComment(log)))
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
