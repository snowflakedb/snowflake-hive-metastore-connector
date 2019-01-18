/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
import com.snowflake.core.commands.DropExternalTable;
import com.snowflake.core.util.StringUtil.SensitiveString;
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
@PowerMockIgnore("javax.management.*")
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

    DropExternalTable dropExternalTable = new DropExternalTable(dropTableEvent);

    List<SensitiveString> commands = dropExternalTable.generateCommands();
    assertEquals("generated drop table command does not match " +
        "expected drop table command",
        "DROP EXTERNAL TABLE t1;",
        commands.get(0).toString());

    assertEquals("generated drop stage command does not match " +
        "expected drop stage command",
        "DROP STAGE t1;",
        commands.get(1).toString());
  }
}
