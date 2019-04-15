/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.util;

import com.snowflake.hive.listener.SnowflakeHiveListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Utility class that allows messages to be queued and processed in the
 * background. Messages are accumulated in the queue, and processed periodically
 *
 * @param <T> The type of message
 * @author wwong
 */
public class BatchScheduler<T>
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeHiveListener.class);

  private int threadPoolCount;

  private int batchingPeriod;

  private BiConsumer<Queue<T>, BatchScheduler<T>> processMessages;

  private ConcurrentLinkedQueue<T> messageQueue = new ConcurrentLinkedQueue<>();

  private ScheduledExecutorService scheduledExecutor = null;

  private ScheduledFuture scheduledFuture = null;

  private ExecutorService threadPool = null;

  /**
   * Constructor for the scheduler
   * @param threadPoolCount Number of worker threads to use
   * @param batchingPeriod Duration of time between processing the batches
   * @param processMessages Method that processes the messages, if given the
   *                        message queue
   */
  public BatchScheduler(int threadPoolCount,
                        int batchingPeriod,
                        BiConsumer<Queue<T>, BatchScheduler<T>> processMessages)
  {
    this.threadPoolCount = threadPoolCount;
    this.batchingPeriod = batchingPeriod;
    this.processMessages = processMessages;
  }

  /**
   * Enqueues a message to be collected and batched
   * @param message the message
   */
  public void enqueueMessage(T message)
  {
    log.info("Enqueueing message. Current count: " + messageQueue.size());
    messageQueue.add(message);
    startIfNotStarted();
  }

  /**
   * Submits a task to a thread pool
   * @param task the task to be executed
   */
  public void submitTask(Runnable task)
  {
    log.info("Task received");
    threadPool.submit(task);
  }

  /**
   * Helper method that instantiates certain resources. Idempotent.
   */
  private void startIfNotStarted()
  {
    if (scheduledFuture != null && !scheduledFuture.isDone())
    {
      log.info("Scheduler already started");
      return;
    }

    if (scheduledFuture != null && scheduledFuture.isDone())
    {
      try
      {
        scheduledFuture.get();
      }
      catch (Exception ex)
      {
        log.warn("Scheduler had stopped earlier due to exception: " + ex);
      }
    }

    if (scheduledExecutor == null)
    {
      log.info("Starting schedule executor");
      scheduledExecutor = Executors.newScheduledThreadPool(1);
    }

    if (threadPool == null)
    {
      log.info(String.format("Instantiating thread pool with %s threads",
                             threadPoolCount));
      threadPool = Executors.newFixedThreadPool(threadPoolCount);
    }

    log.info(String.format("Starting schedule with a batching period of %s ms",
                           batchingPeriod));

    // Notes:
    //  - Executes a recurring action in a timely fashion (no drift)
    //  - If an action takes longer than the period, all future actions are late
    //  - An exception will stop future actions
    scheduledFuture = scheduledExecutor.scheduleAtFixedRate(() ->
    {
      try
      {
        processMessages.accept(messageQueue, this);
      }
      catch (Exception ex)
      {
        log.warn("Hit exception running scheduled action: " + ex);
      }
    }, 0, batchingPeriod, TimeUnit.MILLISECONDS);
    log.info("Started scheduler");
  }
}
