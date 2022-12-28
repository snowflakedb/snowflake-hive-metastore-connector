/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */

import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.SnowflakeHiveListener;
import net.snowflake.hivemetastoreconnector.core.SnowflakeClient;
import net.snowflake.hivemetastoreconnector.util.HiveToSnowflakeSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "jdk.internal.reflect.*"})
@PrepareForTest({Configuration.class, HiveMetaStore.HMSHandler.class,
        SnowflakeClient.class, SnowflakeConf.class,
        SnowflakeHiveListener.class})

/**
 * Miscellaneous Tests for the hive connector
 */
public class MiscTest
{
    /**
     * A test to check if the correct schema from the schema list is chosen
     * @throws Exception
     */
    @Test
    public void verifySchemaSelectionWithQuotedSchemasCommandTest() throws Exception
    {
        Table table = TestUtil.initializeMockTable();

        // Set a new hive db name
        table.setDbName("testSchema");

        // Mock config
        SnowflakeConf mockConfig = TestUtil.initializeMockConfig();

        PowerMockito
                .when(mockConfig.get("snowflake.hive-metastore-listener.stage", null))
                .thenReturn("aStage");

        // Here, the default Snowflake schema will be chosen since the hive db name is not in the schema list
        String chosenSchema = HiveToSnowflakeSchema.getSnowflakeSchemaFromHiveSchema(table.getDbName(), mockConfig);
        assertEquals("Chosen schema does not match the expected default schema",
                "someSchema1",
                chosenSchema);

        // Add testSchema in quotes to the schema list - this should still get matched with the Hive schema/DB name
        PowerMockito
                .when(mockConfig.getStringCollection("snowflake.hive-metastore-listener.schemas"))
                .thenReturn(Arrays.asList(new String[]{"newSchema1", "\"testSchema\""}));

        chosenSchema = HiveToSnowflakeSchema.getSnowflakeSchemaFromHiveSchema(table.getDbName(), mockConfig);
        assertEquals("Chosen schema does not match the expected schema from schema list enclosed in quotes",
                "\"testSchema\"",
                chosenSchema);
    }
}
