import com.snowflake.core.commands.AddPartition;
import com.snowflake.core.util.StringUtil.SensitiveString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
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
    Table table = createMockTable();

    // mock partition
    Partition partition = new Partition();
    partition.setValues(Arrays.asList("1", "testName"));
    partition.setSd(new StorageDescriptor());
    partition.getSd().setLocation("s3n://bucketname/path/to/table/sub/path");

    HiveMetaStore.HMSHandler mockHandler =
        PowerMockito.mock(HiveMetaStore.HMSHandler.class);

    AddPartitionEvent addPartitionEvent =
        new AddPartitionEvent(table, partition, true, mockHandler);

    AddPartition addPartition = new AddPartition(addPartitionEvent);

    List<SensitiveString> commands = addPartition.generateCommands();
    assertEquals("generated add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='1'," +
                     "name='testName') LOCATION 'sub/path' /* TABLE LOCATION " +
                     "= 's3n://bucketname/path/to/table' */;",
                 commands.get(0).toString());
  }

  /**
   * A basic test for generating two add partition commands from one event
   * @throws Exception
   */
  @Test
  public void multiAddPartitionGenerateCommandTest() throws Exception
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

    AddPartitionEvent addPartitionEvent = new AddPartitionEvent(
      table, Arrays.asList(partition1, partition2), true, mockHandler);

    AddPartition addPartition = new AddPartition(addPartitionEvent);

    List<SensitiveString> commands = addPartition.generateCommands();
    assertEquals("first generated add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='1'," +
                     "name='testName') LOCATION 'sub/path' /* TABLE LOCATION " +
                     "= 's3n://bucketname/path/to/table' */;",
                 commands.get(0).toString());
    assertEquals("second generated add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='2'," +
                     "name='testName2') LOCATION 'sub/path2' /* TABLE " +
                     "LOCATION = 's3n://bucketname/path/to/table' */;",
                 commands.get(1).toString());
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
