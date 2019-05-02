/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
import com.snowflake.core.commands.DropPartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Configuration.class, HiveMetaStore.HMSHandler.class})

/**
 * Tests for generating the drop partition command
 */
public class DropPartitionTest
{
  /**
   * A basic test for generating a drop partition command for a simple partition
   * @throws Exception
   */
  @Test
  public void basicDropPartitionGenerateCommandTest() throws Exception
  {
    // mock table
    Table table = createMockTable();

    // mock partition
    Partition partition = new Partition();
    partition.setValues(Arrays.asList("1", "testName"));
    partition.setSd(new StorageDescriptor());
    partition.getSd().setLocation("s3n://bucketname/path/to/table/sub/path");

    HiveMetaStore.HMSHandler mockHandler =
        PowerMockito.mock(HiveMetaStore.HMSHandler.class);

    DropPartitionEvent dropPartitionEvent =
        new DropPartitionEvent(table, partition, true, false, mockHandler);

    DropPartition dropPartition = new DropPartition(dropPartitionEvent);

    List<String> commands = dropPartition.generateStatements();
    assertEquals("generated add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 DROP PARTITION " +
                     "LOCATION 'sub/path' " +
                     "/* TABLE LOCATION = 's3n://bucketname/path/to/table' */;",
                 commands.get(0));
  }

  /**
   * A basic test for generating two drop partition commands from one event
   * @throws Exception
   */
  @Test
  public void multiDropPartitionGenerateCommandTest() throws Exception
  {
    // mock table
    Table table = createMockTable();

    // mock partitions
    Partition partition1 = new Partition();
    partition1.setValues(Arrays.asList("1", "testName"));
    partition1.setSd(new StorageDescriptor());
    partition1.getSd().setLocation("s3n://bucketname/path/to/table/sub/path");

    Partition partition2 = new Partition();
    partition2.setValues(Arrays.asList("2", "testName2"));
    partition2.setSd(new StorageDescriptor());
    partition2.getSd().setLocation("s3n://bucketname/path/to/table/sub/path2");

    HiveMetaStore.HMSHandler mockHandler =
        PowerMockito.mock(HiveMetaStore.HMSHandler.class);

    // There is no public constructor for DropPartitionEvent with multiple
    // partitions- mock this ourselves
    DropPartitionEvent dropPartitionEvent =
        PowerMockito.mock(DropPartitionEvent.class);
    PowerMockito
        .when(dropPartitionEvent.getTable())
        .thenReturn(table);
    PowerMockito
        .when(dropPartitionEvent.getPartitionIterator())
        .thenReturn(Arrays.asList(partition1, partition2).iterator());

    DropPartition dropPartition = new DropPartition(dropPartitionEvent);

    List<String> commands = dropPartition.generateStatements();
    assertEquals("first generated add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 DROP PARTITION " +
                     "LOCATION 'sub/path' " +
                     "/* TABLE LOCATION = 's3n://bucketname/path/to/table' */;",
                 commands.get(0));
    assertEquals("second generated add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 DROP PARTITION " +
                     "LOCATION 'sub/path2' " +
                     "/* TABLE LOCATION = 's3n://bucketname/path/to/table' */;",
                 commands.get(1));
  }

  private static Table createMockTable()
  {
    Table table = new Table();
    table.setTableName("t1");
    table.setPartitionKeys(Arrays.asList(
        new FieldSchema("partcol", "int", null),
        new FieldSchema("name", "string", null)));
    table.setSd(new StorageDescriptor());
    table.getSd().setLocation("s3n://bucketname/path/to/table");

    return table;
  }
}
