/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.snowflake.core.util.CommandBatchRewriter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for the CommandBatchRewriter
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class BatchRewriterTests
{
  @Rule
  public Timeout globalTimeout = new Timeout(TEST_TIMEOUT_MS);

  private static final int TEST_TIMEOUT_MS = 5000;

  /**
   * Basic test for rewriting the following:
   *  a. create stage -> create table -> add partition
   *  b. create stage -> create table -> add two partitions
   *  c. create stage -> create table -> add a partition again with new location
   *
   * This should be rewritten as:
   * create stage -> create table -> add four partitions
   */
  @Test
  public void batchRewriterTests() throws Exception
  {
    String createStage = "CREATE STAGE IF NOT EXISTS someDB__t1 " +
        "URL='s3://bucketname/path/to/table'\n" +
        "credentials=(AWS_KEY_ID='accessKeyId'\n" +
        "AWS_SECRET_KEY='awsSecretKey');";
    String createTable = "CREATE EXTERNAL TABLE IF NOT EXISTS t1" +
        "(old1 INT as (VALUE:c1::INT),old2 STRING as (VALUE:c2::STRING)," +
        "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
        "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
        "partition by (partcol,name)" +
        "partition_type=user_specified location=@someDB__t1 " +
        "file_format=(TYPE=CSV) AUTO_REFRESH=false;";
    String addPartition1 = "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='1'," +
        "name='testName') LOCATION 'sub/path' /* TABLE LOCATION " +
        "= 's3n://bucketname/path/to/table' */;";
    String addPartition2 = "ALTER EXTERNAL TABLE t1 ADD PARTITION" +
        "(partcol='2',name='testName') LOCATION 'sub/path' /* TABLE LOCATION " +
        "= 's3n://bucketname/path/to/table' */;";
    String addPartition3 = "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='1'," +
        "name='testName3') LOCATION 'sub/path' /* TABLE LOCATION " +
        "= 's3n://bucketname/path/to/table' */;";
    String addPartition4 = "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='1'," +
        "name='testName') LOCATION 'sub/path4' /* TABLE LOCATION " +
        "= 's3n://bucketname/path/to/table' */;";

    List<String> batch1 = ImmutableList.of(createStage, createTable, addPartition1);
    List<String> batch2 = ImmutableList.of(createStage, createTable, addPartition2, addPartition3);
    List<String> batch3 = ImmutableList.of(createStage, createTable, addPartition4);

    List<List<String>> commands = CommandBatchRewriter
        .rewriteBatches(ImmutableList.of(
            batch1,
            batch2,
            batch3));

    String expectedBatchedAddPartition = "ALTER EXTERNAL TABLE t1 ADD " +
        "PARTITION(partcol='1',name='testName') LOCATION 'sub/path'" +
        " /* TABLE LOCATION = 's3n://bucketname/path/to/table' */, " +
        "PARTITION (partcol='2',name='testName') LOCATION 'sub/path'" +
        " /* TABLE LOCATION = 's3n://bucketname/path/to/table' */, " +
        "PARTITION (partcol='1',name='testName3') LOCATION 'sub/path'" +
        " /* TABLE LOCATION = 's3n://bucketname/path/to/table' */, " +
        "PARTITION (partcol='1',name='testName') LOCATION 'sub/path4'" +
        " /* TABLE LOCATION = 's3n://bucketname/path/to/table' */;";

    Assert.assertEquals(1, commands.size());

    List<String> batch = commands.get(0);
    Assert.assertEquals(3, batch.size());
    Assert.assertEquals(createStage, batch.get(0));
    Assert.assertEquals(createTable, batch.get(1));
    Assert.assertEquals(expectedBatchedAddPartition, batch.get(2));
  }

  /**
   * Test for different tables by rewriting the following:
   *  a. create stage -> create table -> add partition
   *  b. create second stage -> create second table -> add partition
   *  c. create stage -> create table -> add a partition again with new location
   *  d. create second stage -> create second table -> add partition
   *
   * This should be rewritten as:
   * create stage -> create table -> add two partitions
   * create second stage -> create second table -> add two partitions
   */
  @Test
  public void differentTableBatchRewriterTests() throws Exception
  {
    String createStage = "CREATE STAGE IF NOT EXISTS someDB__t1 " +
        "URL='s3://bucketname/path/to/table'\n" +
        "credentials=(AWS_KEY_ID='accessKeyId'\n" +
        "AWS_SECRET_KEY='awsSecretKey');";
    String createTable = "CREATE EXTERNAL TABLE IF NOT EXISTS t1" +
        "(old1 INT as (VALUE:c1::INT),old2 STRING as (VALUE:c2::STRING)," +
        "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
        "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
        "partition by (partcol,name)" +
        "partition_type=user_specified location=@someDB__t1 " +
        "file_format=(TYPE=CSV) AUTO_REFRESH=false;";
    String createStage2 = "CREATE STAGE IF NOT EXISTS someDB__t2 " +
        "URL='s3://bucketname/path/to/table'\n" +
        "credentials=(AWS_KEY_ID='accessKeyId'\n" +
        "AWS_SECRET_KEY='awsSecretKey');";
    String createTable2 = "CREATE EXTERNAL TABLE IF NOT EXISTS t2" +
        "(old1 INT as (VALUE:c1::INT),old2 STRING as (VALUE:c2::STRING)," +
        "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
        "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
        "partition by (partcol,name)" +
        "partition_type=user_specified location=@someDB__t1 " +
        "file_format=(TYPE=CSV) AUTO_REFRESH=false;";
    String addPartition1 = "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='1'," +
        "name='testName') LOCATION 'sub/path' /* TABLE LOCATION " +
        "= 's3n://bucketname/path/to/table' */;";
    String addPartition2 = "ALTER EXTERNAL TABLE t2 ADD PARTITION" +
        "(partcol='2',name='testName') LOCATION 'sub/path' /* TABLE LOCATION " +
        "= 's3n://bucketname/path/to/table' */;";
    String addPartition3 = "ALTER EXTERNAL TABLE t1 ADD PARTITION" +
        "(partcol='1'," +
        "name='testName') LOCATION 'sub/path4' /* TABLE LOCATION " +
        "= 's3n://bucketname/path/to/table' */;";
    String addPartition4 = "ALTER EXTERNAL TABLE t2 ADD PARTITION(partcol='1'," +
        "name='testName3') LOCATION 'sub/path' /* TABLE LOCATION " +
        "= 's3n://bucketname/path/to/table' */;";

    List<String> batch1 = ImmutableList.of(createStage, createTable, addPartition1);
    List<String> batch2 = ImmutableList.of(createStage2, createTable2, addPartition2);
    List<String> batch3 = ImmutableList.of(createStage, createTable, addPartition3);
    List<String> batch4 = ImmutableList.of(createStage2, createTable2, addPartition4);

    List<List<String>> commands = CommandBatchRewriter
        .rewriteBatches(ImmutableList.of(
            batch1,
            batch2,
            batch3,
            batch4));

    String expectedBatchedAddPartition = "ALTER EXTERNAL TABLE t1 ADD " +
        "PARTITION(partcol='1',name='testName') LOCATION 'sub/path'" +
        " /* TABLE LOCATION = 's3n://bucketname/path/to/table' */, " +
        "PARTITION (partcol='1',name='testName') LOCATION 'sub/path4'" +
        " /* TABLE LOCATION = 's3n://bucketname/path/to/table' */;";

    Assert.assertEquals(2, commands.size());

    List<String> outputBatch = commands.get(0);
    Assert.assertEquals(3, outputBatch.size());
    Assert.assertEquals(createStage, outputBatch.get(0));
    Assert.assertEquals(createTable, outputBatch.get(1));
    Assert.assertEquals(expectedBatchedAddPartition, outputBatch.get(2));

    String expectedBatchedAddPartition2 = "ALTER EXTERNAL TABLE t2 ADD " +
        "PARTITION(partcol='2',name='testName') LOCATION 'sub/path'" +
        " /* TABLE LOCATION = 's3n://bucketname/path/to/table' */, " +
        "PARTITION (partcol='1',name='testName3') LOCATION 'sub/path'" +
        " /* TABLE LOCATION = 's3n://bucketname/path/to/table' */;";

    List<String> outputBatch2 = commands.get(1);
    Assert.assertEquals(3, outputBatch2.size());
    Assert.assertEquals(createStage2, outputBatch2.get(0));
    Assert.assertEquals(createTable2, outputBatch2.get(1));
    Assert.assertEquals(expectedBatchedAddPartition2, outputBatch2.get(2));
  }

  /**
   * Validates that rewriter has reasonable performance for the following case:
   *  a. create stage -> create table -> add partition 1
   *  b. create stage -> create table -> add partition 2
   *  ...
   *
   * This should be rewritten as:
   * create stage -> create table -> add partition 1, 2, ...
   *
   * This case is relevant for a Hive command like 'msck repair table t1;'
   */
  @Test
  public void perfbatchRewriterTests1() throws Exception
  {
    String createStage = "CREATE STAGE IF NOT EXISTS someDB__t1 " +
        "URL='s3://bucketname/path/to/table'\n" +
        "credentials=(AWS_KEY_ID='accessKeyId'\n" +
        "AWS_SECRET_KEY='awsSecretKey');";
    String createTable = "CREATE EXTERNAL TABLE IF NOT EXISTS t1" +
        "(old1 INT as (VALUE:c1::INT),old2 STRING as (VALUE:c2::STRING)," +
        "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
        "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
        "partition by (partcol,name)" +
        "partition_type=user_specified location=@someDB__t1 " +
        "file_format=(TYPE=CSV) AUTO_REFRESH=false;";
    String addPartitionFormat = "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='%s'," +
        "name='testName3') LOCATION 'sub/path' /* TABLE LOCATION " +
        "= 's3n://bucketname/path/to/table' */;";

    List<List<String>> batches = new ArrayList<>();
    for (int i = 0; i < 1000; i++)
    {
      batches.add(ImmutableList.of(createStage, createTable,
                                   String.format(addPartitionFormat,
                                                 i)));
    }

    List<List<String>> commands = CommandBatchRewriter.rewriteBatches(batches);

    Assert.assertEquals(1, commands.size());
  }

  /**
   * Validates that rewriter has reasonable performance for the following case:
   * create stage -> create table -> add partition 1 -> add partition 2 -> ...
   *
   * This should be rewritten as:
   * create stage -> create table -> add partition 1, 2, ...
   *
   * This case is relevant for a Hive command like
   * 'alter table t1 partition(partcol) change column partcol partcol int;'
   */
  @Test
  public void perfbatchRewriterTests2() throws Exception
  {
    String createStage = "CREATE STAGE IF NOT EXISTS someDB__t1 " +
        "URL='s3://bucketname/path/to/table'\n" +
        "credentials=(AWS_KEY_ID='accessKeyId'\n" +
        "AWS_SECRET_KEY='awsSecretKey');";
    String createTable = "CREATE EXTERNAL TABLE IF NOT EXISTS t1" +
        "(old1 INT as (VALUE:c1::INT),old2 STRING as (VALUE:c2::STRING)," +
        "partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
        "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
        "partition by (partcol,name)" +
        "partition_type=user_specified location=@someDB__t1 " +
        "file_format=(TYPE=CSV) AUTO_REFRESH=false;";
    String addPartitionFormat = "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='%s'," +
        "name='testName3') LOCATION 'sub/path' /* TABLE LOCATION " +
        "= 's3n://bucketname/path/to/table' */;";

    List<List<String>> batches = ImmutableList.of(
        Lists.newArrayList(createStage, createTable));
    for (int i = 0; i < 1000; i++)
    {
      batches.get(0).add(String.format(addPartitionFormat, i));
    }

    List<List<String>> commands = CommandBatchRewriter.rewriteBatches(batches);

    Assert.assertEquals(1, commands.size());
  }

  /**
   * Basic test for drop partitions by rewriting the following:
   *  a. drop partition 1
   *  b. drop partition 1
   *  c. drop partition 2 -> drop partition 3
   *
   * This should be rewritten as:
   * drop three partitions
   */
  @Test
  public void dropPartitionsBatchRewriterTests() throws Exception
  {
    String dropPartition1 = "ALTER EXTERNAL TABLE t1 DROP PARTITION" +
        " LOCATION 'sub/path1' /* TABLE LOCATION = 's3n://bucketname/path/to/table' */;";
    String dropPartition2 = "ALTER EXTERNAL TABLE t1 DROP PARTITION" +
        " LOCATION 'sub/path2' /* TABLE LOCATION = 's3n://bucketname/path/to/table' */;";
    String dropPartition3 = "ALTER EXTERNAL TABLE t1 DROP PARTITION" +
        " LOCATION 'sub/path3' /* TABLE LOCATION = 's3n://bucketname/path/to/table' */;";

    List<String> batch1 = ImmutableList.of(dropPartition1);
    List<String> batch2 = ImmutableList.of(dropPartition1);
    List<String> batch3 = ImmutableList.of(dropPartition2, dropPartition3);

    List<List<String>> commands = CommandBatchRewriter
        .rewriteBatches(ImmutableList.of(
            batch1,
            batch2,
            batch3));

    String expectedBatchedDropPartition = "ALTER EXTERNAL TABLE t1 DROP PARTITION " +
        "LOCATION 'sub/path1' /* TABLE LOCATION = 's3n://bucketname/path/to/table' */,  " +
        "LOCATION 'sub/path2' /* TABLE LOCATION = 's3n://bucketname/path/to/table' */,  " +
        "LOCATION 'sub/path3' /* TABLE LOCATION = 's3n://bucketname/path/to/table' */;";

    Assert.assertEquals(1, commands.size());

    List<String> batch = commands.get(0);
    Assert.assertEquals(1, batch.size());
    Assert.assertEquals(expectedBatchedDropPartition, batch.get(0));
  }
}
