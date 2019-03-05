/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.core.commands.AlterTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
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

/**
 * Tests for generating the alter table command
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Configuration.class, HiveMetaStore.HMSHandler.class})
public class AlterTableTest
{
  /**
   * A basic test for generating a alter (touch) table command
   * @throws Exception
   */
  @Test
  public void basicTouchTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    Configuration mockConfig = PowerMockito.mock(Configuration.class);
    PowerMockito.when(mockConfig.get("fs.s3n.awsAccessKeyId"))
        .thenReturn("accessKeyId");
    PowerMockito.when(mockConfig.get("fs.s3n.awsSecretAccessKey"))
        .thenReturn("awsSecretKey");
    HiveMetaStore.HMSHandler mockHandler =
        PowerMockito.mock(HiveMetaStore.HMSHandler.class);
    PowerMockito.when(mockHandler.getConf()).thenReturn(mockConfig);
    AlterTableEvent alterTableEvent = new AlterTableEvent(table, table,
                                                          true, true, mockHandler);

    AlterTable alterTable = new AlterTable(alterTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = alterTable.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE STAGE IF NOT EXISTS someDB_t1 " +
                     "url='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated alter table command does not match " +
                     "expected alter table command",
                 "CREATE EXTERNAL TABLE IF NOT EXISTS t1" +
                     "(partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)" +
                     "location=@someDB_t1 partition_type=user_specified " +
                     "file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(1));
  }
}
