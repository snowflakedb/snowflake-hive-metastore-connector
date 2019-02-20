import com.snowflake.conf.SnowflakeConf;
import com.snowflake.core.commands.CreateExternalTable;
import com.snowflake.core.util.StringUtil.SensitiveString;
import com.snowflake.jdbc.client.SnowflakeClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
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
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
    Table table = initializeMockTable();

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent);

    List<SensitiveString> commands = createExternalTable.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE t1 " +
                     "url='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='{awsSecretKey}');",
                 commands.get(0).toString());

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(partcol INT as " +
                     "(parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)location=@t1 " +
                     "partition_type=user_specified file_format=(TYPE=CSV);",
                 commands.get(1).toString());
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
    Table table = initializeMockTable();

    Map<String, String> serDeParams = new HashMap<>();
    serDeParams.put("field.delim", "','");
    serDeParams.put("line.delim", "'\n'");
    serDeParams.put("escape.delim", "'$'");
    table.getSd().getSerdeInfo().setParameters(serDeParams);

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent);

    List<SensitiveString> commands = createExternalTable.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE t1 " +
                     "url='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='{awsSecretKey}');",
                 commands.get(0).toString());

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                     "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)location=@t1 partition_type=user_specified " +
                     "file_format=(RECORD_DELIMITER=''\n'',FIELD_DELIMITER='','',TYPE=CSV,ESCAPE=''$'');",
                 commands.get(1).toString());
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
    Table table = initializeMockTable();

    table.getSd().setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
    table.getSd().getSerdeInfo().setSerializationLib(
        "parquet.hive.serde.ParquetHiveSerDe");
    Map<String, String> serDeParams = new HashMap<>();
    serDeParams.put("parquet.compression", "snappy");
    table.getSd().getSerdeInfo().setParameters(serDeParams);

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent);

    List<SensitiveString> commands = createExternalTable.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE t1 " +
                     "url='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='{awsSecretKey}');",
                 commands.get(0).toString());

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                     "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)location=@t1 " +
                     "partition_type=user_specified file_format=(TYPE=PARQUET);",
                 commands.get(1).toString());
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
    Table table = initializeMockTable();

    table.getSd().setCols(Arrays.asList(
        new FieldSchema("col1", "int", null),
        new FieldSchema("col2", "string", null)));

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent);

    List<SensitiveString> commands = createExternalTable.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE t1 " +
                     "url='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='{awsSecretKey}');",
                 commands.get(0).toString());

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                     "col1 INT as (VALUE:c1::INT)," +
                     "col2 STRING as (VALUE:c2::STRING)," +
                     "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)location=@t1 " +
                     "partition_type=user_specified file_format=(TYPE=CSV);",
                 commands.get(1).toString());
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
    Table table = initializeMockTable();

    table.getSd().setCols(Arrays.asList(
        new FieldSchema("col1", "int", null),
        new FieldSchema("col2", "string", null)));
    table.getSd().setInputFormat("parquet.hive.DeprecatedParquetInputFormat");
    table.getSd().getSerdeInfo().setSerializationLib(
        "parquet.hive.serde.ParquetHiveSerDe");

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent);

    List<SensitiveString> commands = createExternalTable.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE t1 " +
                     "url='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='{awsSecretKey}');",
                 commands.get(0).toString());

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                     "col1 INT as (VALUE:col1::INT)," +
                     "col2 STRING as (VALUE:col2::STRING)," +
                     "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)location=@t1 " +
                     "partition_type=user_specified file_format=(TYPE=PARQUET);",
                 commands.get(1).toString());
  }

  /**
   * Tests the error handling of the client during a create table event
   * @throws Exception
   */
  @Test
  public void retryCreateTableGenerateCommandTest() throws Exception
  {
    Table table = initializeMockTable();

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, initializeMockHMSHandler());

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
    SnowflakeConf mockConfig = PowerMockito.mock(SnowflakeConf.class);
    PowerMockito
        .when(mockConfig.getInt("snowflake.hivemetastorelistener.retry.timeout", 1000))
        .thenReturn(0);
    PowerMockito
        .when(mockConfig.getInt("snowflake.hivemetastorelistener.retry.count", 3))
        .thenReturn(3);

    // Execute an event
    SnowflakeClient.createAndExecuteEventForSnowflake(createTableEvent,
                                                      mockConfig);

    // Count the number of times each query was executed. They should have
    // executed twice each.
    Mockito
        .verify(mockStatement, Mockito.times(4))
        .executeQuery(anyString());

    Mockito
        .verify(mockStatement, Mockito.times(2))
        .executeQuery("CREATE OR REPLACE STAGE t1 " +
                          "url='s3://bucketname/path/to/table'" +
                          "\ncredentials=(AWS_KEY_ID='accessKeyId'" +
                          "\nAWS_SECRET_KEY='awsSecretKey');");
    Mockito
        .verify(mockStatement, Mockito.times(2))
        .executeQuery(
            "CREATE OR REPLACE EXTERNAL TABLE t1(" +
                "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                "partition by (partcol,name)" +
                "location=@t1 " +
                "partition_type=user_specified " +
                "file_format=(TYPE=CSV);");
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
    Table table = initializeMockTable();
    table.getPartitionKeys().forEach(
        fieldSchema -> fieldSchema.setType("NOT A VALID TYPE"));

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent);

    List<SensitiveString> commands = createExternalTable.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE t1 " +
                     "url='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='{awsSecretKey}');",
                 commands.get(0).toString());

    assertEquals(
        "generated create external table command does not match " +
          "expected create external table command",
        "CREATE OR REPLACE EXTERNAL TABLE t1(partcol VARIANT as " +
          "(parse_json(metadata$external_table_partition):PARTCOL::VARIANT)," +
          "name VARIANT as " +
          "(parse_json(metadata$external_table_partition):NAME::VARIANT))" +
          "partition by (partcol,name)location=@t1 " +
          "partition_type=user_specified file_format=(TYPE=CSV);",
                 commands.get(1).toString());
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
    Table table = initializeMockTable();
    table.getSd().setLocation("INVALID PROTOCOL://bucketname/path/to/table");

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, initializeMockHMSHandler());

    CreateExternalTable createExternalTable =
        new CreateExternalTable(createTableEvent);

    List<SensitiveString> commands = createExternalTable.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE OR REPLACE STAGE t1 " +
                     "url='INVALID PROTOCOL://bucketname/path/to/table'\n" +
                     "credentials=(/* Error generating credentials " +
                       "expression: The stage type does not exist or is " +
                       "unsupported for URL: INVALID " +
                       "PROTOCOL://bucketname/path/to/table */);",
                 commands.get(0).toString());

    assertEquals("generated create external table command does not match " +
                     "expected create external table command",
                 "CREATE OR REPLACE EXTERNAL TABLE t1(partcol INT as " +
                     "(parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)location=@t1 " +
                     "partition_type=user_specified file_format=(TYPE=CSV);",
                 commands.get(1).toString());
  }

  /**
   * Negative test for the error handling of command generation itself
   * @throws Exception
   */
  @Test
  public void logErrorCreateTableGenerateCommandTest() throws Exception
  {
    Table table = initializeMockTable();
    table.getSd().setInputFormat("NOT A VALID FORMAT");
    table.getSd().getSerdeInfo().setSerializationLib("NOT A VALID SERDE");

    CreateTableEvent createTableEvent =
        new CreateTableEvent(table, true, initializeMockHMSHandler());

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
    SnowflakeConf mockConfig = PowerMockito.mock(SnowflakeConf.class);
    PowerMockito
        .when(mockConfig.getInt("snowflake.hivemetastorelistener.retry.timeout", 1000))
        .thenReturn(0);
    PowerMockito
        .when(mockConfig.getInt("snowflake.hivemetastorelistener.retry.count", 3))
        .thenReturn(3);

    // Execute an event
    SnowflakeClient.createAndExecuteEventForSnowflake(createTableEvent,
                                                      mockConfig);

    Mockito
        .verify(mockStatement, Mockito.times(1)); // No retries
        String expectedSubtring = "SELECT NULL /* HIVE METASTORE LISTENER ERROR " +
          "(javax.transaction.NotSupportedException): 'Snowflake does not " +
          "support the corresponding SerDe: NOT A VALID SERDE'\n" +
          "STACKTRACE: 'javax.transaction.NotSupportedException: Snowflake does" +
          " not support the corresponding SerDe: NOT A VALID SERDE\n";
    assertEquals(1, executeQueryParams.size());
    assertTrue("Invocation does not contain the expected substring",
               executeQueryParams.get(0).contains(expectedSubtring));
  }

  /**
   * Helper class to initialize the Hive metastore handler, which is commonly
   * used for tests in this class.
   */
  private HiveMetaStore.HMSHandler initializeMockHMSHandler()
  {
    // Mock the HMSHandler and configurations
    Configuration mockConfig = PowerMockito.mock(Configuration.class);
    HiveMetaStore.HMSHandler mockHandler =
        PowerMockito.mock(HiveMetaStore.HMSHandler.class);
    PowerMockito.when(mockConfig.get("fs.s3n.awsAccessKeyId"))
        .thenReturn("accessKeyId");
    PowerMockito.when(mockConfig.get("fs.s3n.awsSecretAccessKey"))
        .thenReturn("awsSecretKey");
    PowerMockito.when(mockHandler.getConf()).thenReturn(mockConfig);

    return mockHandler;
  }

  /**
   * Helper class to initialize a base Table object for tests
   */
  private Table initializeMockTable()
  {
    Table table = new Table();

    table.setTableName("t1");
    table.setPartitionKeys(Arrays.asList(
        new FieldSchema("partcol", "int", null),
        new FieldSchema("name", "string", null)));
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(new ArrayList<>());
    table.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    table.getSd().setLocation("s3n://bucketname/path/to/table");
    table.getSd().setSerdeInfo(new SerDeInfo());
    table.getSd().getSerdeInfo().setSerializationLib(
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    table.getSd().getSerdeInfo().setParameters(new HashMap<>());
    table.setParameters(new HashMap<>());

    return table;
  }
}
