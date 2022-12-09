/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
import net.snowflake.hivemetastoreconnector.commands.DropExternalTable;
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

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for generating the drop table command
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "jdk.internal.reflect.*"})
@PrepareForTest({Configuration.class, HiveMetaStore.HMSHandler.class})
public class DropTableTest
{
  /**
   * A basic test for generating a drop table command
   * @throws Exception
   */
  @Test
  public void basicDropTableGenerateCommandTest() throws Exception
  {
    Table table = new Table();

    table.setTableName("t1");

    HiveMetaStore.HMSHandler mockHandler =
        PowerMockito.mock(HiveMetaStore.HMSHandler.class);
    DropTableEvent dropTableEvent = new DropTableEvent(table,
        true, true, mockHandler);

    DropExternalTable dropExternalTable =
        new DropExternalTable(dropTableEvent,
                              TestUtil.initializeMockConfig());

    List<String> commands = dropExternalTable.generateSqlQueries();
    assertEquals("generated drop table command does not match " +
        "expected drop table command",
        "DROP EXTERNAL TABLE IF EXISTS t1;",
        commands.get(0));

    assertEquals("generated drop stage command does not match " +
        "expected drop stage command",
        "DROP STAGE IF EXISTS someDB__t1;",
        commands.get(1));
  }
}
