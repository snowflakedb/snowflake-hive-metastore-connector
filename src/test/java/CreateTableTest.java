import com.snowflake.core.commands.CreateExternalTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
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
 * Tests for generating the create table command
 */
public class CreateTableTest
{
  /**
   * A basic test for generating a create table command for a simple table
   * @throws Exception
   */
  @Test
  public void basicCreateTableGenerateCommandTest() throws Exception
  {
    // create table
    Table table = new Table();

    table.setTableName("t1");
    table.setPartitionKeys(Arrays.asList(
        new FieldSchema("partcol", "int", null),
        new FieldSchema("name", "string", null)));
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(new ArrayList<FieldSchema>());
    table.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    table.getSd().setLocation("s3n://bucketname/path/to/table");
    table.getSd().setSerdeInfo(new SerDeInfo());
    table.getSd().getSerdeInfo().setParameters(new HashMap<>());

    // Mock the HMSHandler and configurations
    Configuration mockConfig = PowerMockito.mock(Configuration.class);
    HiveMetaStore.HMSHandler mockHandler =
        PowerMockito.mock(HiveMetaStore.HMSHandler.class);
    PowerMockito.when(mockConfig.get("fs.s3n.awsAccessKeyId"))
        .thenReturn("{accessKeyId}");
    PowerMockito.when(mockConfig.get("fs.s3n.awsSecretAccessKey"))
        .thenReturn("{secretAccessKey}");
    PowerMockito.when(mockHandler.getConf()).thenReturn(mockConfig);

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, mockHandler);

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent);

    List<String> commands = createExternalTable.generateCommands();
    assertEquals("generated create stage command does not match " +
        "expected create stage command",
          "CREATE STAGE t1 url='s3://bucketname/path/to/table'\n" +
          "credentials=(AWS_KEY_ID='{accessKeyId}'\n" +
          "AWS_SECRET_KEY='{secretAccessKey}');",
        commands.get(0));

    assertEquals("generated create external table command does not match " +
        "expected create external table command",
        "CREATE EXTERNAL TABLE t1(partcol INT as " +
            "(parse_json(metadata$external_table_partition):PARTCOL::INT)," +
            "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
            "partition by (partcol,name)location=@t1 " +
            "partition_type=user_specified file_format=(TYPE=CSV);",
        commands.get(1));
  }
}
