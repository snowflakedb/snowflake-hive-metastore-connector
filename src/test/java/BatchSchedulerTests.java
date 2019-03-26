/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
import com.google.common.collect.ImmutableList;
import com.snowflake.core.util.BatchScheduler;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the BatchScheduler
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class BatchSchedulerTests
{
  @Rule
  public Timeout globalTimeout = new Timeout(TEST_TIMEOUT_MS);

  private static final int TEST_TIMEOUT_MS = 5000;

  /**
   * A basic test for scheduling messages and tasks
   * @throws Exception
   */
  @Test
  public void basicCommandExecution() throws Exception
  {
    List<String> toEnqueue = ImmutableList.of("a", "b", "c");
    List<String> dequeuedMessages = new ArrayList<>();
    List<String> submittedTasks = new ArrayList<>();

    BatchScheduler<String> batchScheduler = new BatchScheduler<>(1, 10,
    (queue, scheduler) ->
    {
      while (!queue.isEmpty())
      {
        String message = queue.remove();
        dequeuedMessages.add(message);
        scheduler.submitTask(() -> submittedTasks.add(message));
      }
    });

    toEnqueue.forEach(batchScheduler::enqueueMessage);
    waitUntilCondition(() -> dequeuedMessages.size() == toEnqueue.size());
    waitUntilCondition(() -> submittedTasks.size() == toEnqueue.size());

    // Verify
    assertArrayEquals(toEnqueue.toArray(), dequeuedMessages.toArray());
    assertArrayEquals(toEnqueue.toArray(), submittedTasks.toArray());
  }

  /**
   * Verifies serial execution when setting thread pool count to 1.
   * Does the following:
   *  - Queue 3 messages
   *  - After the second message is processed and submitted, block the thread
   *  - Wait a short duration
   *  - Check that the third message was not processed
   *
   * @throws Exception
   */
  @Test
  public void serialCommandExecution() throws Exception
  {
    List<String> toEnqueue = ImmutableList.of("a", "b", "c");
    List<String> dequeuedMessages = new ArrayList<>();
    List<String> submittedTasks = new ArrayList<>();

    BatchScheduler<String> batchScheduler = new BatchScheduler<>(1, 10,
    (queue, scheduler) ->
    {
      if (!queue.isEmpty())
      {
        String message = queue.remove();
        dequeuedMessages.add(message);
        scheduler.submitTask(() -> submittedTasks.add(message));
      }

      if (dequeuedMessages.size() > 1)
      {
        waitUntilCondition(() -> false);
      }
    });

    toEnqueue.forEach(batchScheduler::enqueueMessage);
    waitUntilCondition(() -> dequeuedMessages.size() == 2);
    Thread.sleep(100);

    // Verify
    assertEquals(2, dequeuedMessages.size());
    assertEquals(2, submittedTasks.size());
  }

  /**
   * Verifies concurrent execution when setting thread pool count to 3.
   *
   * Does the following:
   * - Queues 3 events, which when processed, blocks the thread
   * - Waits a short amount of time
   * - Verifies that all of them were started, but none finished
   * @throws Exception
   */
  @Test
  public void concurrentCommandExecution() throws Exception
  {
    List<String> toEnqueue = ImmutableList.of("a", "b", "c");
    List<String> dequeuedMessages = new ArrayList<>();
    List<String> submittedTasks = new ArrayList<>();
    List<String> completedTasks = new ArrayList<>();

    BatchScheduler<String> batchScheduler = new BatchScheduler<>(3, 10,
    (queue, scheduler) ->
    {
      while (!queue.isEmpty())
      {
        String message = queue.remove();
        dequeuedMessages.add(message);
        scheduler.submitTask(() ->
        {
          submittedTasks.add(message);
          waitUntilCondition(() -> false);
          completedTasks.add(message);
        });
      }
    });

    toEnqueue.forEach(batchScheduler::enqueueMessage);
    waitUntilCondition(() -> submittedTasks.size() == 3);
    Thread.sleep(100);

    // Verify
    assertEquals(3, dequeuedMessages.size());
    assertEquals(3, submittedTasks.size());
    assertEquals(0, completedTasks.size());
  }

  /**
   * Helper method to wait for a condition to be true
   * @param condition when this condition is true, stop waiting
   */
  private static void waitUntilCondition(Supplier<Boolean> condition)
  {
    for (int i = 0; i < TEST_TIMEOUT_MS; i+=10)
    {
      if (condition.get())
      {
        break;
      }

      try
      {
        Thread.sleep(10);
      }
      catch (InterruptedException e)
      {
        Thread.currentThread().interrupt();
      }
    }
  }
}

