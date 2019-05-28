/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.hive.listener.SnowflakeHiveListener;
import com.snowflake.jdbc.client.SnowflakeClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

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
public class SnowflakeHiveListenerTests
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
}
