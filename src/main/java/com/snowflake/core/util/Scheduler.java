/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.util;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.Lists;
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.core.commands.AddPartition;
import com.snowflake.core.commands.Command;
import com.snowflake.hive.listener.SnowflakeHiveListener;
import com.snowflake.jdbc.client.SnowflakeClient;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Utility class that allows messages to be queued and processed in the
 * background. Messages are accumulated in a queue, and processed whenever
 * possible by a thread pool
 *
 * Internally, there is a queue for each table and a thread pool to process
 * messages in each queue. When a new queue is created, a task is submitted
 * to the thread pool to do a unit of work. If there are still items in the
 * queue after the unit of work, the task will submit a continuation task to
 * repeat this process. If there are errors in processing the queue, the
 * we will skip that item and continue with the rest of the queue.
 *
 * @author wwong
 */
public class Scheduler
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeHiveListener.class);

  // Mapping between a table and a queue of messages for that table.
  // When a queue is initialized, a task is also created. Therefore, the only
  // time this cache may be accessed is while enqueueing a messages.
  private final LoadingCache<TableKey, BlockingQueue<Command>> messageQueues;

  // The worker pool
  private final ExecutorService threadPool;

  // To prevent other queues from starving, only execute a certain number of
  // statements per 'round' of processing.
  private static final int MAX_STATEMENTS_PER_ROUND = 10;

  // The Snowflake configuration
  private final SnowflakeConf snowflakeConf;

  /**
   * Constructor for the scheduler
   * @param threadPoolCount Number of worker threads to use
   */
  public Scheduler(int threadPoolCount, SnowflakeConf snowflakeConf)
  {
    Preconditions.checkArgument(threadPoolCount > 0);
    Preconditions.checkNotNull(snowflakeConf);
    this.threadPool = Executors.newScheduledThreadPool(threadPoolCount);
    this.snowflakeConf = snowflakeConf;
    this.messageQueues = CacheBuilder.newBuilder()
        .removalListener(
            (RemovalListener<TableKey, BlockingQueue<Command>>)
                removal -> log.info(String.format("Removing queue %s from cache",
                                                  removal.getKey())))
        .build(
            new CacheLoader<TableKey, BlockingQueue<Command>>()
            {
              @ParametersAreNonnullByDefault
              public BlockingQueue<Command> load(TableKey key)
              {
                // Submit a task when a queue is created. This ensures that
                // a task is created for each queue in the cache.
                LinkedBlockingQueue<Command> queue = new LinkedBlockingQueue<>();
                threadPool.submit(() -> doWork(key, queue));
                return queue;
              }
            });
  }

  /**
   * Enqueues a message to be collected and batched
   * @param message the message
   */
  public void enqueueMessage(Command message)
  {
    Preconditions.checkNotNull(message);
    Queue<Command> messageQueue;
    try
    {
      // If there is no key in the cache, a queue will be initialized
      messageQueue = messageQueues.get(getKeyFromMessage(message));
    }
    catch (ExecutionException e)
    {
      log.error("Could not initialize queue " + e);
      return;
    }
    Preconditions.checkNotNull(messageQueue);
    log.info("Enqueueing message. Current count (before enqueuing): " + messageQueue.size());
    messageQueue.add(message);
  }

  /**
   * Helper method that does some work and queue up more work if necessary
   * @param key The key associated with the queue
   * @param queue The queue to process
   */
  private void doWork(TableKey key, LinkedBlockingQueue<Command> queue)
  {
    try
    {
      boolean continueWork = processMessages(queue, snowflakeConf);
      if (continueWork)
      {
        threadPool.submit(() -> doWork(key, queue));
      }
      else
      {
        // It's possible that an item is enqueued right before the queue is
        // invalidated. We expect this to be exceedingly rare, so we'll just
        // re-enqueue those messages in a new queue, knowing that the order
        // may be incorrect if new messages come in before the messages are
        // re-enqueued.
        messageQueues.invalidate(key);

        List<Command> remaining = Lists.newArrayList();
        queue.drainTo(remaining);
        remaining.forEach(this::enqueueMessage);
      }
    }
    catch (Throwable t)
    {
      log.error("Encountered error while processing queue: " + t);

      // Skip the previous work and continue with new work
      threadPool.submit(() -> doWork(key, queue));
    }
  }

  /**
   * Process messages in a queue with the same key
   * @param messages the messages in the queue
   * @param snowflakeConf the Snowflake configuration
   * @return whether there is still more work to be done
   */
  private static boolean processMessages(BlockingQueue<Command> messages,
                                         SnowflakeConf snowflakeConf)
  {
    Command command = null;

    // Combine add partition commands as we iterate along the queue.
    // This command is the previously saved command
    AddPartition partitionsToAdd = null;

    // Execute N statements on a table at a time.
    int commandsExecuted = 0;

    do
    {
      // Dequeue a command, but if a command can't be dequeued for any reason
      // (timeout or thread interrupt), stop processing commands
      try
      {
        // We expect that there is exactly one consumer for each queue, so
        // there should still be an item in the queue. Nevertheless, we have
        // a timeout, just in case.
        command = messages.poll(10, TimeUnit.SECONDS);
      }
      catch (InterruptedException e)
      {
        log.error("Thread interrupted: " + e);
        Thread.currentThread().interrupt();
      }
      if (command == null)
      {
        return false;
      }

      List<Command> commands = Lists.newArrayList(command);

      if (command instanceof AddPartition)
      {
        // Combine the current command and the saved one (if there was one)
        commands = Lists.newArrayList(AddPartition.combinedOf(
            partitionsToAdd, (AddPartition) command));
        Preconditions.checkNotNull(commands);
        Preconditions.checkState(!commands.isEmpty()
                                     && commands.size() <= 2);

        if (!(messages.peek() instanceof AddPartition))
        {
          // If the next command can't be combined, execute everything we've saved
          partitionsToAdd = null;
        }
        else
        {
          // Otherwise, save the last un-combined command for later
          partitionsToAdd = (AddPartition)commands.remove(commands.size() - 1);
        }
      }

      commands.forEach(
          c -> SnowflakeClient.generateAndExecuteSnowflakeStatements(c, snowflakeConf));
      commandsExecuted += commands.size();

      // Relinquish this thread after a certain number of commands have been
      // processed.
      if (commandsExecuted >= MAX_STATEMENTS_PER_ROUND)
      {
        return !messages.isEmpty();
      }
    }
    while (!messages.isEmpty());

    log.info("Queue processed.");
    return false;
  }

  /**
   * Convenience method to create a key from a queue message
   * @param message the queue message
   * @return a key which determines which queue to enqueue the message to
   */
  private TableKey getKeyFromMessage(Command message)
  {
    return new TableKey(message.getDatabaseName(), message.getTableName());
  }

  /**
   * Helper class that represents a key that identifies which queue a message
   * should be enqueued to.
   */
  private static class TableKey
  {
    private final String databaseName;

    private final String tableName;

    TableKey(String databaseName, String tableName)
    {
      Preconditions.checkNotNull(databaseName);
      Preconditions.checkNotNull(tableName);
      this.databaseName = databaseName;
      this.tableName = tableName;
    }

    @Override
    public int hashCode()
    {
      return new HashCodeBuilder().append(databaseName).append(tableName).build();
    }

    public String toString()
    {
      return String.format("%s.%s", databaseName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof TableKey
          && databaseName.equals(((TableKey) obj).databaseName)
          && tableName.equals(((TableKey) obj).tableName);
    }
  }
}
