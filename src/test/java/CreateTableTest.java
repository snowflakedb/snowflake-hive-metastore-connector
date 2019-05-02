/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.core.commands.CreateExternalTable;
import com.snowflake.jdbc.client.SnowflakeClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.sql.RowSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Configuration.class, HiveMetaStore.HMSHandler.class,
                DriverManager.class, Consumer.class, SnowflakeClient.class})

/**
 * Tests for generating the create table command
 */
public class CreateTableTest
{
  /**
   * A basic test for generating a create table command for a simple table
   *
   * @throws Exception
   */
  @Test
  public void basicCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(partcol INT as " +
                     "(parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)partition_type=user_specified " +
                     "location=@someDB__t1 file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(1));
  }

  /**
   * A test for generating a create table command for a table with file format
   * type of CSV.
   *
   * @throws Exception
   */
  @Test
  public void csvCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    Map<String, String> serDeParams = new HashMap<>();
    serDeParams.put("field.delim", ",");
    serDeParams.put("line.delim", "\n");
    serDeParams.put("escape.delim", "$");
    table.getSd().getSerdeInfo().setParameters(serDeParams);

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                     "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)partition_type=user_specified location=@someDB__t1 " +
                     "file_format=(RECORD_DELIMITER='\\n',FIELD_DELIMITER=',',TYPE=CSV,ESCAPE='$') " +
                     "AUTO_REFRESH=false;",
                 commands.get(1));
  }

  /**
   * A test for generating a create table command for a table with file format
   * type of CSV. Uses OpenCSV SerDe properties
   *
   * @throws Exception
   */
  @Test
  public void openCsvCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    Map<String, String> serDeParams = new HashMap<>();
    serDeParams.put("separatorChar", ",");
    serDeParams.put("escapeChar", "\\");
    serDeParams.put("quoteChar", "\"");
    table.getSd().getSerdeInfo().setParameters(serDeParams);

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = createExternalTable.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                     "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)partition_type=user_specified location=@someDB__t1 " +
                     "file_format=(FIELD_DELIMITER=',',TYPE=CSV,ESCAPE='\\\\',FIELD_OPTIONALLY_ENCLOSED_BY='\\\"') " +
                     "AUTO_REFRESH=false;",
                 commands.get(1));
  }

  /**
   * A test for generating a create table command for a table with file format
   * type of Parquet.
   *
   * @throws Exception
   */
  @Test
  public void parquetCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    table.getSd().setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
    table.getSd().getSerdeInfo().setSerializationLib(
        "parquet.hive.serde.ParquetHiveSerDe");
    Map<String, String> serDeParams = new HashMap<>();
    serDeParams.put("parquet.compression", "snappy");
    table.getSd().getSerdeInfo().setParameters(serDeParams);

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                     "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)partition_type=user_specified " +
                     "location=@someDB__t1 file_format=(TYPE=PARQUET) " +
                     "AUTO_REFRESH=false;",
                 commands.get(1));
  }

  /**
   * A test for generating a create table command for a table with additional
   * columns that aren't partition columns.
   *
   * @throws Exception
   */
  @Test
  public void multiColumnCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    table.getSd().setCols(Arrays.asList(
        new FieldSchema("col1", "int", null),
        new FieldSchema("col2", "string", null)));

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                     "col1 INT as (VALUE:c1::INT)," +
                     "col2 STRING as (VALUE:c2::STRING)," +
                     "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)partition_type=user_specified " +
                     "location=@someDB__t1 file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(1));
  }

  /**
   * A test for generating a create table command for a table with additional
   * columns that aren't partition columns. Uses Parquet.
   *
   * @throws Exception
   */
  @Test
  public void parquetMultiColumnCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    table.getSd().setCols(Arrays.asList(
        new FieldSchema("col1", "int", null),
        new FieldSchema("col2", "string", null)));
    table.getSd().setInputFormat("parquet.hive.DeprecatedParquetInputFormat");
    table.getSd().getSerdeInfo().setSerializationLib(
        "parquet.hive.serde.ParquetHiveSerDe");

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                     "col1 INT as (VALUE:col1::INT)," +
                     "col2 STRING as (VALUE:col2::STRING)," +
                     "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)partition_type=user_specified " +
                     "location=@someDB__t1 file_format=(TYPE=PARQUET) " +
                     "AUTO_REFRESH=false;",
                 commands.get(1));
  }

  /**
   * A test for creating an unpartitioned table and refreshing it
   * @throws Exception
   */
  @Test
  public void unpartitionedTouchTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();
    table.setPartitionKeys(new ArrayList<>());
    table.getSd().setCols(Arrays.asList(
        new FieldSchema("col1", "int", null),
        new FieldSchema("col2", "string", null)));
    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated alter table command does not match " +
                     "expected alter table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                     "col1 INT as (VALUE:c1::INT),col2 STRING as (VALUE:c2::STRING))" +
                     "partition_type=implicit location=@someDB__t1 " +
                     "file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(1));
    assertEquals("generated alter table command does not match " +
                     "expected alter table command",
                 "ALTER EXTERNAL TABLE t1 REFRESH;",
                 commands.get(2));
  }

  /**
   * A test for generating a create table command for a table with an
   * existing stage.
   *
   * @throws Exception
   */
  @Test
  public void existingStageCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    // Mock config
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito
        .when(mockConfig.get("snowflake.hive-metastore-listener.stage", null))
        .thenReturn("aStage");

    // Mock Snowflake client to return a location for this stage
    TestUtil.mockSnowflakeStageWithLocation("s3://bucketname/path");

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, mockConfig);

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(partcol INT as " +
                     "(parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)partition_type=user_specified " +
                     "location=@aStage/to/table file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(0));
    assertEquals("Unexpected number of commands generated", 1, commands.size());
  }

  /**
   * A test for generating a create table command for a table with an
   * existing stage. Tests the case that the stage name and path are the same
   *
   * @throws Exception
   */
  @Test
  public void existingStageSamePathCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    // Mock config
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito
        .when(mockConfig.get("snowflake.hive-metastore-listener.stage", null))
        .thenReturn("aStage");

    // Mock Snowflake client to return a location for this stage
    TestUtil.mockSnowflakeStageWithLocation("s3://bucketname/path/to/table");

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, mockConfig);

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(partcol INT as " +
                     "(parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)partition_type=user_specified " +
                     "location=@aStage/ file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(0));
    assertEquals("Unexpected number of commands generated", 1, commands.size());
  }

  /**
   * A test for generating a create table command for a table with an
   * existing stage. Tests the case that the stage name and path are the same
   *
   * @throws Exception
   */
  @Test
  public void existingStageInvalidCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    // Mock config
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito
        .when(mockConfig.get("snowflake.hive-metastore-listener.stage", null))
        .thenReturn("aStage");

    // Mock Snowflake client to return a location for this stage
    TestUtil.mockSnowflakeStageWithLocation("s3://bucketname2");

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, mockConfig);

    boolean threwCorrectException = false;
    try
    {
      createExternalTable.generateSqlQueries();
    }
    catch (IllegalArgumentException ex)
    {
      assertEquals("The table location must be a subpath of the stage location. " +
                       "tableLocation: 's3://bucketname/path/to/table', " +
                       "stageLocation: 's3://bucketname2'",
                   ex.getMessage());
      threwCorrectException = true;
    }
    assertTrue(threwCorrectException);
  }

  /**
   * A test for generating a create table command for a table with an
   * integration
   *
   * @throws Exception
   */
  @Test
  public void integrationCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    // Mock config
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito
        .when(mockConfig.get("snowflake.hive-metastore-listener.integration",
                             null))
        .thenReturn("anIntegration");

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, mockConfig);

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "STORAGE_INTEGRATION=anIntegration;",
                 commands.get(0));
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(partcol INT as " +
                     "(parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)partition_type=user_specified " +
                     "location=@someDB__t1 file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(1));
    assertEquals("Unexpected number of commands generated", 2, commands.size());
  }

  /**
   * A test for generating a create table command for a table with a column
   * of type VARCHAR(200), CHAR(200), DECIMAL(38,0)
   *
   * @throws Exception
   */
  @Test
  public void varcharCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    table.getSd().setCols(Arrays.asList(
        new FieldSchema("col1", "varchar(200)", null),
        new FieldSchema("col2", "char(200)", null),
        new FieldSchema("col3", "decimal(38,0)", null)));

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                     "col1 VARCHAR(200) as (VALUE:c1::VARCHAR(200))," +
                     "col2 CHAR(200) as (VALUE:c2::CHAR(200))," +
                     "col3 DECIMAL(38,0) as (VALUE:c3::DECIMAL(38,0))," +
                     "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)partition_type=user_specified " +
                     "location=@someDB__t1 file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(1));
  }

  /**
   * Tests the error handling of the client during a create table event
   * @throws Exception
   */
  @Test
  public void retryCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    // Mock JDBC connection to be unreliable during query execution
    RowSet mockRowSet = PowerMockito.mock(RowSet.class);
    PowerMockito.when(mockRowSet.next()).thenReturn(false);

    Statement mockStatement = PowerMockito.mock(Statement.class);
    PowerMockito
        .when(mockStatement.executeQuery(anyString()))
        .thenThrow(new SQLException())
        .thenReturn(mockRowSet)
        .thenThrow(new SQLException())
        .thenReturn(mockRowSet);

    Connection mockConnection = PowerMockito.mock(Connection.class);
    PowerMockito
        .when(mockConnection.createStatement())
        .thenReturn(mockStatement);

    PowerMockito.mockStatic(DriverManager.class);
    PowerMockito
        .when(DriverManager.getConnection(any(String.class),
                                          any(Properties.class)))
        .thenReturn(mockConnection);

    // Mock configuration to have a wait time of zero (so tests are quick)
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();

    // Execute an event
    SnowflakeClient.createAndExecuteCommandForSnowflake(
        createTableEvent, mockConfig);

    // Count the number of times each query was executed. They should have
    // executed twice each.
    Mockito
        .verify(mockStatement, Mockito.times(4))
        .executeQuery(anyString());

    Mockito
        .verify(mockStatement, Mockito.times(2))
        .executeQuery("CREATE OR REPLACE STAGE someDB__t1 " +
                          "URL='s3://bucketname/path/to/table'" +
                          "\ncredentials=(AWS_KEY_ID='accessKeyId'" +
                          "\nAWS_SECRET_KEY='awsSecretKey');");
    Mockito
        .verify(mockStatement, Mockito.times(2))
        .executeQuery(
            "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                "partition by (partcol,name)" +
                "partition_type=user_specified " +
                "location=@someDB__t1 " +
                "file_format=(TYPE=CSV) AUTO_REFRESH=false;");
  }

  /**
   * A negative test for generating a create table command with invalid columns
   * Expects column types to fallback to VARIANT, everything is normal otherwise
   *
   * @throws Exception
   */
  @Test
  public void columnErrorCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();
    table.getPartitionKeys().forEach(
        fieldSchema -> fieldSchema.setType("NOT A VALID TYPE"));

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals(
        "generated create external table command does not match " +
          "expected create external table command",
        "CREATE OR REPLACE EXTERNAL TABLE t1(partcol VARIANT as " +
          "(parse_json(metadata$external_table_partition):PARTCOL::VARIANT)," +
          "name VARIANT as " +
          "(parse_json(metadata$external_table_partition):NAME::VARIANT))" +
          "partition by (partcol,name)partition_type=user_specified " +
          "location=@someDB__t1 file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(1));
  }

  /**
   * A negative test for generating a create table command with an invalid URL
   * Expects credentials to be empty, everything is normal otherwise.
   *
   * @throws Exception
   */
  @Test
  public void credentialsErrorCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();
    table.getSd().setLocation("INVALID PROTOCOL://bucketname/path/to/table");

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = createExternalTable.generateSqlQueries();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE someDB__t1 " +
                     "URL='INVALID PROTOCOL://bucketname/path/to/table'\n" +
                     "credentials=(/* Error generating credentials " +
                       "expression: The stage type does not exist or is " +
                       "unsupported for URL: INVALID " +
                       "PROTOCOL://bucketname/path/to/table */);",
                 commands.get(0));

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(partcol INT as " +
                     "(parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)partition_type=user_specified " +
                     "location=@someDB__t1 file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(1));
  }

  /**
   * A negative test for generating a create table command with no stage
   * specified and disabling reading from Hive conf. The user's configuration
   * should not be read if this flag is not enabled.
   *
   * @throws Exception
   */
  @Test
  public void stageErrorCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();

    HiveMetaStore.HMSHandler hmsHandler = TestUtil.initializeMockHMSHandler();
    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, hmsHandler);

    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();
    PowerMockito
        .when(mockConfig.getBoolean("snowflake.hive-metastore-listener.enable-creds-from-conf", false))
        .thenReturn(false);
    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent, mockConfig);

    try
    {
      createExternalTable.generateSqlQueries();
      fail("Command generation did not fail when it should have.");
    }
    catch (IllegalArgumentException ex)
    {
      assertEquals("Configuration does not specify a stage to use. Add a " +
                       "configuration for snowflake.hive-metastore-listener.stage to specify the stage.",
                   ex.getMessage());
    }

    // No attempts to read from config
    Configuration mockHiveConfig = hmsHandler.getConf();
    Mockito
        .verify(mockHiveConfig, Mockito.times(0))
        .get(any());
    Mockito
        .verify(mockHiveConfig, Mockito.times(0));
  }

  /**
   * Negative test for the error handling of command generation itself
   * @throws Exception
   */
  @Test
  public void noOpErrorCreateTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();
    table.getSd().setInputFormat("NOT A VALID FORMAT");
    table.getSd().getSerdeInfo().setSerializationLib("NOT A VALID SERDE");

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, TestUtil.initializeMockHMSHandler());

    // Mock JDBC connection to be unreliable during query execution
    RowSet mockRowSet = PowerMockito.mock(RowSet.class);
    PowerMockito.when(mockRowSet.next()).thenReturn(false);

    List<String> executeQueryParams = new ArrayList<>();
    Statement mockStatement = PowerMockito.mock(Statement.class);
    PowerMockito
        .when(mockStatement.executeQuery(anyString()))
        .thenAnswer((Answer<RowSet>) invocation ->
        {
          executeQueryParams.addAll(Arrays.asList(Arrays.copyOf(
          invocation.getArguments(), invocation.getArguments().length, String[].class)));
          return mockRowSet;
        });

    Connection mockConnection = PowerMockito.mock(Connection.class);
    PowerMockito
        .when(mockConnection.createStatement())
        .thenReturn(mockStatement);

    PowerMockito.mockStatic(DriverManager.class);
    PowerMockito
        .when(DriverManager.getConnection(any(String.class),
                                          any(Properties.class)))
        .thenReturn(mockConnection);

    // Mock configuration to have a wait time of zero (so tests are quick)
    SnowflakeConf mockConfig = TestUtil.initializeMockConfig();

    // Execute an event
    SnowflakeClient.createAndExecuteCommandForSnowflake(
        createTableEvent, mockConfig);

    Mockito
        .verify(mockStatement, Mockito.times(0)); // No retries
    assertEquals(0, executeQueryParams.size());
  }
}
