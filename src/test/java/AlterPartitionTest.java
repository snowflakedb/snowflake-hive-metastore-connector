import com.snowflake.conf.SnowflakeConf;
import com.snowflake.core.commands.AlterPartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Configuration.class, HiveMetaStore.HMSHandler.class})

/**
 * Tests for generating the alter partition command
 */
public class AlterPartitionTest
{
  /**
   * A basic test for generating an alter partition command for a simple
   * partition
   * @throws Exception
   */
  @Test
  public void basicAlterPartitionGenerateCommandTest() throws Exception
  {
    // mock table
    Table table = TestUtil.initializeMockTable();

    // mock partition
    Partition partition = new Partition();
    partition.setValues(Arrays.asList("1", "testName"));
    partition.setSd(new StorageDescriptor());
    partition.getSd().setLocation("s3n://bucketname/path/to/table/sub/path");

    AlterPartitionEvent alterPartitionEvent =
        new AlterPartitionEvent(partition, partition, table, true,
                                true, TestUtil.initializeMockHMSHandler());

    AlterPartition alterPartition = new AlterPartition(alterPartitionEvent,
                                                       TestUtil.initializeMockConfig());

    List<String> commands = alterPartition.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE STAGE IF NOT EXISTS someDB_t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated alter table command does not match " +
                     "expected alter table command",
                 "CREATE EXTERNAL TABLE IF NOT EXISTS t1" +
                     "(partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)" +
                     "partition_type=user_specified location=@someDB_t1 " +
                     "file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(1));

    assertEquals("generated add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='1'," +
                     "name='testName') LOCATION 'sub/path' /* TABLE LOCATION " +
                     "= 's3n://bucketname/path/to/table' */;",
                 commands.get(2));
  }
}
