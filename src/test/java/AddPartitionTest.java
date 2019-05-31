/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.core.commands.AddPartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Configuration.class, HiveMetaStore.HMSHandler.class,
                    SnowflakeConf.class})

/**
 * Tests for generating the add partition command
 */
public class AddPartitionTest
{
  /**
   * A basic test for generating an add partition command for a simple partition
   * @throws Exception
   */
  @Test
  public void basicAddPartitionGenerateCommandTest() throws Exception
  {
    // mock table
    Table table = TestUtil.initializeMockTable();

    // mock partition
    Partition partition = new Partition();
    partition.setValues(Arrays.asList("1", "testName"));
    partition.setSd(new StorageDescriptor());
    partition.getSd().setLocation("s3n://bucketname/path/to/table/sub/path");

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();

    AddPartitionEvent addPartitionEvent =
        new AddPartitionEvent(table, partition, true, mockHandler);

    AddPartition addPartition = new AddPartition(addPartitionEvent, TestUtil.initializeMockConfig());

    List<String> commands = addPartition.generateSqlQueries();
    assertEquals(3, commands.size());
    assertEquals("generated add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='1'," +
                     "name='testName') LOCATION 'sub/path' /* TABLE LOCATION " +
                     "= 's3n://bucketname/path/to/table' */;",
                 commands.get(2));
  }

  /**
   * A basic test for generating two add partition commands from one event
   * @throws Exception
   */
  @Test
  public void multiAddPartitionGenerateCommandTest() throws Exception
  {
    // mock table
    Table table = TestUtil.initializeMockTable();

    // mock partitions
    Partition partition1 = new Partition();
    partition1.setValues(Arrays.asList("1", "testName"));
    partition1.setSd(new StorageDescriptor());
    partition1.getSd().setLocation("s3n://bucketname/path/to/table/sub/path");

    Partition partition2 = new Partition();
    partition2.setValues(Arrays.asList("2", "testName2"));
    partition2.setSd(new StorageDescriptor());
    partition2.getSd().setLocation("s3n://bucketname/path/to/table/sub/path2");

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();

    AddPartitionEvent addPartitionEvent = new AddPartitionEvent(
      table, Arrays.asList(partition1, partition2), true, mockHandler);

    AddPartition addPartition = new AddPartition(addPartitionEvent, TestUtil.initializeMockConfig());

    List<String> commands = addPartition.generateSqlQueries();
    assertEquals(3, commands.size());
    assertEquals("add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 ADD " +
                     "PARTITION(partcol='1',name='testName') " +
                     "LOCATION 'sub/path', " +
                     "PARTITION(partcol='2',name='testName2') " +
                     "LOCATION 'sub/path2' " +
                     "/* TABLE LOCATION = 's3n://bucketname/path/to/table' */;",
                 commands.get(2));
  }

  /**
   * Testing the combining functionality of add partition commands
   */
  @Test
  public void combineAddPartitionGenerateCommandTest() throws Exception
  {
    // mock table
    Table table = TestUtil.initializeMockTable();

    // mock partitions
    Partition partition1 = new Partition();
    partition1.setValues(Arrays.asList("1", "testName"));
    partition1.setSd(new StorageDescriptor());
    partition1.getSd().setLocation("s3n://bucketname/path/to/table/sub/path");

    Partition partition2 = new Partition();
    partition2.setValues(Arrays.asList("2", "testName2"));
    partition2.setSd(new StorageDescriptor());
    partition2.getSd().setLocation("s3n://bucketname/path/to/table/sub/path2");

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();

    AddPartitionEvent addPartitionEvent1 = new AddPartitionEvent(
        table, Collections.singletonList(partition1), true, mockHandler);
    AddPartitionEvent addPartitionEvent2 = new AddPartitionEvent(
        table, Collections.singletonList(partition2), true, mockHandler);

    AddPartition addPartition1 = new AddPartition(addPartitionEvent1,
                                                  TestUtil.initializeMockConfig());
    AddPartition addPartition2 = new AddPartition(addPartitionEvent2,
                                                  TestUtil.initializeMockConfig());

    List<AddPartition> combined = AddPartition.compact(ImmutableList.of(addPartition1,
                                                                        addPartition2));
    assertEquals(1, combined.size());
    assertEquals("add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 ADD " +
                     "PARTITION(partcol='1',name='testName') " +
                     "LOCATION 'sub/path', " +
                     "PARTITION(partcol='2',name='testName2') " +
                     "LOCATION 'sub/path2' " +
                     "/* TABLE LOCATION = 's3n://bucketname/path/to/table' */;",
                 combined.get(0).generateSqlQueries().get(2));
  }

  /**
   * Testing the combining functionality of add partition commands
   */
  @Test
  public void combineOverflowAddPartitionGenerateCommandTest() throws Exception
  {
    // mock table
    Table table = TestUtil.initializeMockTable();

    // mock partitions
    Partition partition1 = new Partition();
    partition1.setValues(Arrays.asList("1", "testName"));
    partition1.setSd(new StorageDescriptor());
    partition1.getSd().setLocation("s3n://bucketname/path/to/table/sub/path");

    Partition partition2 = new Partition();
    partition2.setValues(Arrays.asList("2", "testName2"));
    partition2.setSd(new StorageDescriptor());
    partition2.getSd().setLocation("s3n://bucketname/path/to/table/sub/path2");

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();

    AddPartitionEvent addPartitionEvent1 = new AddPartitionEvent(
        table, Collections.singletonList(partition1), true, mockHandler);
    List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < 101; i++)
    {
      partitions.add(partition2);
    }
    AddPartitionEvent addPartitionEvent2 = new AddPartitionEvent(
        table, partitions, true, mockHandler);

    AddPartition addPartition1 = new AddPartition(addPartitionEvent1,
                                                  TestUtil.initializeMockConfig());
    AddPartition addPartition2 = new AddPartition(addPartitionEvent2,
                                                  TestUtil.initializeMockConfig());

    List<AddPartition> combined = AddPartition.compact(ImmutableList.of(addPartition1,
                                                                        addPartition2));
    assertEquals(2, combined.size());
    assertTrue("add partition command does not match " +
                     "expected add partition command",
               combined.get(0).generateSqlQueries().get(2).startsWith(
               "ALTER EXTERNAL TABLE t1 ADD " +
                     "PARTITION(partcol='1',name='testName') " +
                     "LOCATION 'sub/path', " +
                     "PARTITION(partcol='2',name='testName2') " +
                     "LOCATION 'sub/path2', " +
                     "PARTITION(partcol='2',name='testName2') " +
                     "LOCATION 'sub/path2', " +
                     "PARTITION(partcol='2',name='testName2') " +
                     "LOCATION 'sub/path2', " +
                     "PARTITION(partcol='2',name='testName2') " +
                     "LOCATION 'sub/path2', "
                                                                     ));
    assertEquals("add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 ADD " +
                     "PARTITION(partcol='2',name='testName2') " +
                     "LOCATION 'sub/path2', " +
                     "PARTITION(partcol='2',name='testName2') " +
                     "LOCATION 'sub/path2' " +
                     "/* TABLE LOCATION = 's3n://bucketname/path/to/table' */;",
                 combined.get(1).generateSqlQueries().get(2));
  }

  /**
   * Negative test with a Hive default partition (__HIVE_DEFAULT_PARTITION__)
   * @throws Exception
   */
  @Test
  public void defaultPartitionAddPartitionGenerateCommandTest() throws Exception
  {
    // mock table
    Table table = TestUtil.initializeMockTable();

    // mock partition
    Partition partition = new Partition();
    partition.setValues(Arrays.asList("1", "__HIVE_DEFAULT_PARTITION__"));
    partition.setSd(new StorageDescriptor());
    partition.getSd().setLocation("s3n://bucketname/path/to/table/sub/path");

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();

    AddPartitionEvent addPartitionEvent =
        new AddPartitionEvent(table, partition, true, mockHandler);

    AddPartition addPartition = new AddPartition(addPartitionEvent, TestUtil.initializeMockConfig());

    List<String> commands = addPartition.generateSqlQueries();
    assertEquals(3, commands.size());
    assertEquals("generated add partition command does not match " +
                     "expected add partition command",
                 "SELECT NULL /* No partitions to add. */;",
                 commands.get(2));
  }

  /**
   * Negative test with a Hive default partition (__HIVE_DEFAULT_PARTITION__)
   * @throws Exception
   */
  @Test
  public void defaultPartitionMultiAddPartitionGenerateCommandTest() throws Exception
  {
    // mock table
    Table table = TestUtil.initializeMockTable();

    // mock partitions
    Partition partition1 = new Partition();
    partition1.setValues(Arrays.asList("1", "__HIVE_DEFAULT_PARTITION__"));
    partition1.setSd(new StorageDescriptor());
    partition1.getSd().setLocation("s3n://bucketname/path/to/table/sub/path");

    Partition partition2 = new Partition();
    partition2.setValues(Arrays.asList("2", "testName"));
    partition2.setSd(new StorageDescriptor());
    partition2.getSd().setLocation("s3n://bucketname/path/to/table/sub/path2");

    List<Partition> partitions = ImmutableList.of(partition1, partition2);

    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();

    AddPartitionEvent addPartitionEvent =
        new AddPartitionEvent(table, partitions, true, mockHandler);

    AddPartition addPartition = new AddPartition(addPartitionEvent, TestUtil.initializeMockConfig());

    List<String> commands = addPartition.generateSqlQueries();
    assertEquals(3, commands.size());
    assertEquals("generated add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='2'," +
                     "name='testName') LOCATION 'sub/path2' /* TABLE LOCATION" +
                     " " +
                     "= 's3n://bucketname/path/to/table' */;",
                 commands.get(2));
  }
}
