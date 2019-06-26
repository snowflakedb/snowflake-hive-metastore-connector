/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
import com.google.common.collect.ImmutableList;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.commands.DropPartition;
import net.snowflake.hivemetastoreconnector.core.HiveSyncTool;
import net.snowflake.hivemetastoreconnector.core.SnowflakeClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

/**
 * Unit tests for the sync tool
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Configuration.class, HiveMetaStore.HMSHandler.class,
                    DriverManager.class, SnowflakeConf.class,
                    SnowflakeClient.class, HiveSyncTool.class})
public class HiveSyncToolTest
{
  /**
   * A basic test for the sync tool
   *
   * @throws Exception
   */
  @Test
  public void basicSyncTest() throws Exception
  {
    // Mock the following via the HiveMetaStoreClient:
    // db1:
    //  - tbl1:
    //     - partition 1
    //     - partition 2
    //  - tbl2 (empty)
    // db2 (empty)
    HiveMetaStoreClient mockHmsClient = PowerMockito.mock(HiveMetaStoreClient.class);
    PowerMockito
        .when(mockHmsClient.getAllDatabases())
        .thenReturn(ImmutableList.of("db1", "db2"));
    PowerMockito
        .when(mockHmsClient.getAllTables("db1"))
        .thenReturn(ImmutableList.of("tbl1", "tbl2"));
    PowerMockito
        .when(mockHmsClient.getAllTables("db2"))
        .thenReturn(ImmutableList.of());

    // Mock tbl1
    Table tbl1 = TestUtil.initializeMockTable();
    PowerMockito
        .when(mockHmsClient.getTable("db1", "tbl1"))
        .thenReturn(tbl1);
    tbl1.setTableName("tbl1");
    tbl1.setDbName("db1");
    tbl1.getSd().setLocation("s3://path");

    // Mock tbl2
    Table tbl2 = TestUtil.initializeMockTable();
    PowerMockito
        .when(mockHmsClient.getTable("db1", "tbl2"))
        .thenReturn(tbl2);
    tbl2.setTableName("tbl2");
    tbl2.setDbName("db1");
    tbl2.getSd().setLocation("s3://path");

    // Mock partitions
    Partition partition1 = new Partition();
    partition1.setSd(new StorageDescriptor());
    partition1.getSd().setLocation("s3://path/to/part1");
    Partition partition2 = new Partition();
    partition2.setSd(new StorageDescriptor());
    partition2.getSd().setLocation("s3://path/to/part2");
    List<Partition> mockPartitions1 = ImmutableList.of(partition1, partition2);
    List<Partition> mockPartitions2 = ImmutableList.of();
    PowerMockito
        .when(mockHmsClient.listPartitions("db1", "tbl1", (short) -1))
        .thenReturn(mockPartitions1);
    PowerMockito
        .when(mockHmsClient.listPartitions("db1", "tbl2", (short) -1))
        .thenReturn(mockPartitions2);

    // Mock the following via the SnowflakeClient:
    // db1:
    //  - tbl1:
    //    - partition 2
    //    - partition 3
    ResultSet mockResultSet = PowerMockito.mock(ResultSet.class);
    Mockito.when(mockResultSet.next())
        .thenReturn(true) // tbl1 has 2 files
        .thenReturn(true)
        .thenReturn(false); // tbl2 has no files
    Mockito.when(mockResultSet.getString(1))
        .thenReturn("to/part2/file")
        .thenReturn("to/part3/file");
    ResultSetMetaData mockMetaData = PowerMockito.mock(ResultSetMetaData.class);
    Mockito.when(mockMetaData.getColumnCount())
        .thenReturn(1);
    Mockito.when(mockResultSet.getMetaData())
        .thenReturn(mockMetaData);
    PowerMockito.mockStatic(SnowflakeClient.class);
    PowerMockito.doReturn(mockResultSet).when(SnowflakeClient.class);
    SnowflakeClient.executeStatement(any(), any());
    AtomicInteger numInvocations = new AtomicInteger();
    PowerMockito.doAnswer((Answer) invocation ->
    {
      Object[] args = invocation.getArguments();
      DropPartition cmd = (DropPartition)args[0];
      List<String> queries = cmd.generateSqlQueries();
      if (numInvocations.getAndIncrement() == 0)
      {
        assertEquals(1, queries.size());
        assertEquals("ALTER EXTERNAL TABLE tbl1 DROP PARTITION LOCATION " +
                         "'to/part3' /* TABLE LOCATION = 's3://path' */;",
                     queries.get(0));
      }
      else
      {
        assertEquals(0, queries.size());
      }
      return null;
    }).when(SnowflakeClient.class);
    SnowflakeClient.generateAndExecuteSnowflakeStatements(any(), any());

    // Run the tool
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito
        .whenNew(SnowflakeConf.class).withAnyArguments().thenReturn(mockConfig);
    new HiveSyncTool(mockHmsClient).sync();

    // Verify:
    //  - tables touched
    //  - partition 3 is dropped from table 1
    //  - all partitions are touched
    Mockito
        .verify(mockHmsClient, Mockito.times(1))
        .alter_table("db1", "tbl1", tbl1);
    Mockito
        .verify(mockHmsClient, Mockito.times(1))
        .alter_table("db1", "tbl2", tbl2);

    assertEquals(2, numInvocations.get());

    Mockito
        .verify(mockHmsClient, Mockito.times(2))
        .alter_partitions(any(), any(), any());
    Mockito
        .verify(mockHmsClient)
        .alter_partitions("db1", "tbl1", mockPartitions1);
    Mockito
        .verify(mockHmsClient)
        .alter_partitions("db1", "tbl2", mockPartitions2);
  }
}
