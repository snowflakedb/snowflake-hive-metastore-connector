/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
import com.google.common.collect.ImmutableList;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.SnowflakeHiveListener;
import net.snowflake.hivemetastoreconnector.core.SnowflakeClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;

/**
 * Tests for the SnowflakeHiveListener
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Configuration.class, HiveMetaStore.HMSHandler.class,
                    SnowflakeClient.class, SnowflakeConf.class,
                    SnowflakeHiveListener.class})
public class SnowflakeHiveListenerTest
{
  /**
   * A basic test for handling an event
   * @throws Exception
   */
  @Test
  public void basicDropTable() throws Exception
  {
    Table table = new Table();
    table.setTableName("t1");
    table.setDbName("db1");

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();
    DropTableEvent dropTableEvent = new DropTableEvent(table,
                                                       true, true, mockHandler);

    // Power Mockito cannot mix matchers and non-matchers. Get the config here
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito.whenNew(SnowflakeConf.class).withAnyArguments().thenReturn(mockConfig);
    SnowflakeHiveListener listener =
        new SnowflakeHiveListener(mockHandler.getConf());

    // Mock the static class SnowflakeClient
    PowerMockito.mockStatic(SnowflakeClient.class);
    PowerMockito.doNothing().when(SnowflakeClient.class);
    SnowflakeClient.createAndExecuteCommandForSnowflake(any(), any());

    listener.onDropTable(dropTableEvent);

    // Verify
    PowerMockito.verifyStatic(times(1));
    SnowflakeClient.createAndExecuteCommandForSnowflake(any(), any());
  }

  /**
   * A test to check if the listener respects the table filter regex
   * @throws Exception
   */
  @Test
  public void tableFilterDropTable() throws Exception
  {
    Table table = new Table();
    table.setTableName("t1");
    table.setDbName("db1");

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();
    DropTableEvent dropTableEvent = new DropTableEvent(table,
                                                       true, true, mockHandler);

    // Add a table filter regex via config, which doesn't match the table name
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito
        .when(mockConfig.getPattern(
            "snowflake.hive-metastore-listener.table-filter-regex", null))
        .thenReturn(Pattern.compile(".*"));
    PowerMockito.whenNew(SnowflakeConf.class).withAnyArguments().thenReturn(mockConfig);

    SnowflakeHiveListener listener =
        new SnowflakeHiveListener(mockHandler.getConf());

    // Mock the static class SnowflakeClient
    PowerMockito.mockStatic(SnowflakeClient.class);
    PowerMockito.doNothing().when(SnowflakeClient.class);
    SnowflakeClient.createAndExecuteCommandForSnowflake(any(), any());

    listener.onDropTable(dropTableEvent);

    // Verify
    PowerMockito.verifyStatic(times(0));
    SnowflakeClient.createAndExecuteCommandForSnowflake(any(), any());
  }

  /**
   * A test to check if the listener respects the table filter regex
   * @throws Exception
   */
  @Test
  public void tableFilterNoDropTable() throws Exception
  {
    Table table = new Table();
    table.setTableName("t1");
    table.setDbName("db1");

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();
    DropTableEvent dropTableEvent = new DropTableEvent(table,
                                                       true, true, mockHandler);

    // Add a table filter regex via config, which doesn't match the table name
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito
        .when(mockConfig.getPattern(
            "snowflake.hive-metastore-listener.table-filter-regex", null))
        .thenReturn(Pattern.compile("not t1"));
    PowerMockito.whenNew(SnowflakeConf.class).withAnyArguments().thenReturn(mockConfig);

    SnowflakeHiveListener listener =
        new SnowflakeHiveListener(mockHandler.getConf());

    // Mock the static class SnowflakeClient
    PowerMockito.mockStatic(SnowflakeClient.class);
    PowerMockito.doNothing().when(SnowflakeClient.class);
    SnowflakeClient.createAndExecuteCommandForSnowflake(any(), any());

    listener.onDropTable(dropTableEvent);

    // Verify
    PowerMockito.verifyStatic(times(1));
    SnowflakeClient.createAndExecuteCommandForSnowflake(any(), any());
  }

  /**
   * A test to check if the listener respects the database filter regex
   * @throws Exception
   */
  @Test
  public void databaseFilterDropTable() throws Exception
  {
    Table table = new Table();
    table.setTableName("t1");
    table.setDbName("db1");

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();
    DropTableEvent dropTableEvent = new DropTableEvent(table,
                                                       true, true, mockHandler);

    // Add a database filter regex via config, which doesn't match the database name
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito
        .when(mockConfig.getPattern(
            "snowflake.hive-metastore-listener.database-filter-regex", null))
        .thenReturn(Pattern.compile(".*"));
    PowerMockito.whenNew(SnowflakeConf.class).withAnyArguments().thenReturn(mockConfig);

    SnowflakeHiveListener listener =
        new SnowflakeHiveListener(mockHandler.getConf());

    // Mock the static class SnowflakeClient
    PowerMockito.mockStatic(SnowflakeClient.class);
    PowerMockito.doNothing().when(SnowflakeClient.class);
    SnowflakeClient.createAndExecuteCommandForSnowflake(any(), any());

    listener.onDropTable(dropTableEvent);

    // Verify
    PowerMockito.verifyStatic(times(0));
    SnowflakeClient.createAndExecuteCommandForSnowflake(any(), any());
  }

  /**
   * A test to check if the listener respects the database filter regex
   * @throws Exception
   */
  @Test
  public void databaseFilterNoDropTable() throws Exception
  {
    Table table = new Table();
    table.setTableName("t1");
    table.setDbName("db1");

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();
    DropTableEvent dropTableEvent = new DropTableEvent(table,
                                                       true, true, mockHandler);

    // Add a database filter regex via config, which doesn't match the database name
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito
        .when(mockConfig.getPattern(
            "snowflake.hive-metastore-listener.database-filter-regex", null))
        .thenReturn(Pattern.compile("not db1"));
    PowerMockito.whenNew(SnowflakeConf.class).withAnyArguments().thenReturn(mockConfig);

    SnowflakeHiveListener listener =
        new SnowflakeHiveListener(mockHandler.getConf());

    // Mock the static class SnowflakeClient
    PowerMockito.mockStatic(SnowflakeClient.class);
    PowerMockito.doNothing().when(SnowflakeClient.class);
    SnowflakeClient.createAndExecuteCommandForSnowflake(any(), any());

    listener.onDropTable(dropTableEvent);

    // Verify
    PowerMockito.verifyStatic(times(1));
    SnowflakeClient.createAndExecuteCommandForSnowflake(any(), any());
  }

  /**
   * A sanity test for adding multiple partitions using background processing
   * of events. Also tests basic batching
   * @throws Exception
   */
  @Test(timeout=10000)
  public void basicBackgroundAddPartitions() throws Exception
  {
    Table table = TestUtil.initializeMockTable();
    table.getSd().setLocation("s3://path");

    // Mock partitions
    Partition partition1 = new Partition();
    partition1.setValues(Arrays.asList("1", "testName"));
    partition1.setSd(new StorageDescriptor());
    partition1.getSd().setLocation("s3://path/to/part1");
    Partition partition2 = new Partition();
    partition2.setValues(Arrays.asList("2", "testName"));
    partition2.setSd(new StorageDescriptor());
    partition2.getSd().setLocation("s3://path/to/part2");
    Partition partition3 = new Partition();
    partition3.setValues(Arrays.asList("3", "testName"));
    partition3.setSd(new StorageDescriptor());
    partition3.getSd().setLocation("s3://path/to/part3");

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();

    // Power Mockito cannot mix matchers and non-matchers. Get the config here
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito
        .when(mockConfig.getBoolean("snowflake.hive-metastore-listener.force-synchronous", false))
        .thenReturn(false);
    PowerMockito.whenNew(SnowflakeConf.class).withAnyArguments().thenReturn(mockConfig);
    SnowflakeHiveListener listener =
        new SnowflakeHiveListener(mockHandler.getConf());

    List<AddPartitionEvent> hmsEvents = ImmutableList.of(
        new AddPartitionEvent(table, partition1, true, mockHandler),
        new AddPartitionEvent(table, partition2, true, mockHandler),
        new AddPartitionEvent(table, partition3, true, mockHandler));

    // Make the first statement wait until all events are queued, then make the
    // test wait until all statements are executed
    CountDownLatch eventProcessingGuard = new CountDownLatch(1);
    CountDownLatch testGuard = new CountDownLatch(1);

    List<String> statementsExecuted = new ArrayList<>();
    ResultSet resultSet = PowerMockito.mock(ResultSet.class);
    Statement statement = PowerMockito.mock(Statement.class);
    PowerMockito.when(statement.executeQuery(any())).thenAnswer(invocation ->
    {
      String stmt = (String) invocation.getArguments()[0];
      statementsExecuted.add(stmt);
      eventProcessingGuard.await(); // deadlock iff using sync processing
      if (stmt.contains("PARTITION(partcol='3',name='testName') LOCATION 'to/part3'"))
      {
        testGuard.countDown();
      }
      return resultSet;
    });
    Connection connection = PowerMockito.mock(Connection.class);
    PowerMockito.when(connection.createStatement()).thenReturn(statement);
    PowerMockito.mockStatic(DriverManager.class);
    PowerMockito.when(DriverManager.getConnection(any(), any())).thenReturn(connection);

    // Send events to listener
    hmsEvents.forEach(listener::onAddPartition);
    eventProcessingGuard.countDown();
    testGuard.await();

    // Because the statements are compacted before we can ensure all events
    // are sent to the listener, we can't guarantee which commands are
    // batched, only that one or more commands were batched
    Assert.assertTrue(statementsExecuted.size() <= 6);
    Assert.assertTrue(statementsExecuted.stream().anyMatch(
        stmt -> stmt.contains("PARTITION(partcol='1',name='testName') LOCATION 'to/part1'")));
    Assert.assertTrue(statementsExecuted.stream().anyMatch(
        stmt -> stmt.contains("PARTITION(partcol='2',name='testName') LOCATION 'to/part2'")));
    Assert.assertTrue(statementsExecuted.stream().anyMatch(
        stmt -> stmt.contains("PARTITION(partcol='3',name='testName') LOCATION 'to/part3'")));
  }
}
