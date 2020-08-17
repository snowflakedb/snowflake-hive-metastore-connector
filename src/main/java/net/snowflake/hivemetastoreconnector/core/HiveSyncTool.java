/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.core;

import com.google.common.base.Preconditions;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.commands.AddPartition;
import net.snowflake.hivemetastoreconnector.commands.CreateExternalTable;
import net.snowflake.hivemetastoreconnector.commands.DropPartition;
import net.snowflake.hivemetastoreconnector.util.HiveToSnowflakeSchema;
import net.snowflake.hivemetastoreconnector.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class for syncing metadata from Hive to Snowflake
 * @author wwong
 */
public class HiveSyncTool
{
  private static final Logger log = LoggerFactory.getLogger(HiveSyncTool.class);

  // Note: HiveMetaStoreClient is not backwards compatible. Notably, 2.X
  //       versions use a HiveConf argument instead of a Configuration
  //       argument. Also, if the version of the HiveMetaStoreClient is not
  //       compatible with the metastore, calls to the metastore may return
  //       wrong results, e.g. client.getAllDatabases() returns no databases.
  private final HiveMetaStoreClient hmsClient;

  // The Snowflake configuration
  private final SnowflakeConf snowflakeConf;

  /**
   * Instantiates the HiveSyncTool.
   * @param hmsClient a client for the Hive metastore
   */
  public HiveSyncTool(HiveMetaStoreClient hmsClient)
  {
    this.hmsClient = Preconditions.checkNotNull(hmsClient);
    this.snowflakeConf = new SnowflakeConf();
  }

  /**
   * Does a one-time, one-way metadata sync from the Hive metastore to
   * Snowflake.
   * 1. Tables in Hive are created in Snowflake
   * 2. Partitions not in Hive are dropped from Snowflake
   * 3. Partitions in Hive are added to Snowflake
   * Note: does not drop tables from Snowflake.
   * @throws TException thrown when encountering a Thrift exception while
   *         communicating with the metastore or executing a metastore operation
   */
  public void sync() throws TException
  {
    Pattern tableNameFilter = snowflakeConf.getPattern(
        SnowflakeConf.ConfVars.SNOWFLAKE_TABLE_FILTER_REGEX.getVarname(), null);
    Pattern databaseNameFilter = snowflakeConf.getPattern(
        SnowflakeConf.ConfVars.SNOWFLAKE_DATABASE_FILTER_REGEX.getVarname(), null);
    Set<String> schemaSet = HiveToSnowflakeSchema.getSnowflakeSchemaSet(snowflakeConf);
    String defaultSchema = HiveToSnowflakeSchema.getSnowflakeDefaultSchema(snowflakeConf);

    log.info("Starting sync");
    List<String> databaseNames = hmsClient.getAllDatabases().stream()
        .filter(db -> databaseNameFilter == null || !databaseNameFilter.matcher(db).matches())
        .collect(Collectors.toList());
    log.info(String.format("Syncing %s databases from Hive",
                           databaseNames.size()));
    for (String databaseName : databaseNames)
    {
      Preconditions.checkNotNull(databaseName);
      String schema =
          HiveToSnowflakeSchema.getSnowflakeSchemaFromHiveSchema(databaseName, defaultSchema, schemaSet);
      List<String> tableNames = hmsClient.getAllTables(databaseName).stream()
          .filter(table -> tableNameFilter == null || !tableNameFilter.matcher(table).matches())
          .collect(Collectors.toList());
      log.info(String.format("Syncing %s tables for database %s",
                             tableNames.size(), databaseName));
      for (String tableName : tableNames)
      {
        Preconditions.checkNotNull(tableName);

        // Add missing tables to Snowflake
        Table hiveTable = hmsClient.getTable(databaseName, tableName);
        SnowflakeClient.generateAndExecuteSnowflakeStatements(
            new CreateExternalTable(hiveTable,
                                    snowflakeConf,
                                    new Configuration(), // We won't need Hive configs
                                    false // Don't replace
            ),
            snowflakeConf);

        if (!hiveTable.getPartitionKeys().isEmpty())
        {
          // Drop extra partitions
          dropExtraPartitionsFromSnowflake(databaseName, hiveTable, schema);

          // Add the partitions
          List<Partition> partitions = hmsClient.listPartitions(
              databaseName, tableName, (short) -1 /* all partitions */);
          log.info(String.format("Syncing %s partitions for table %s.%s",
                                 partitions.size(), tableName, databaseName));
          if (partitions.isEmpty())
          {
            log.info(String.format("No need to add partitions for table %s",
                                   tableName));
          }
          else
          {
            SnowflakeClient.generateAndExecuteSnowflakeStatements(
                new AddPartition(hiveTable,
                                 partitions.iterator(),
                                 snowflakeConf,
                                 new Configuration(), // We won't need Hive configs
                                 false // Not compact
                ),
                snowflakeConf);
          }
        }
      }
    }
    log.info("Sync complete");
  }

  /**
   * Helper method that drops extra partitions from Snowflake if they are
   * not in Hive.
   * @throws TException thrown when encountering a Thrift exception while
   *         communicating with the metastore or executing a metastore operation
   */
  private void dropExtraPartitionsFromSnowflake(String databaseName,
                                                Table hiveTable,
                                                String schema)
      throws TException
  {
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(Preconditions.checkNotNull(hiveTable).getTableName());

    // Get Snowflake partition locations
    Set<String> sfPartLocs;
    try
    {
      sfPartLocs = getSnowflakePartitionLocations(hiveTable, schema);
    }
    catch (IllegalStateException | SQLException ex)
    {
      log.warn(String.format(
          "Error encountered, skipping dropping partitions for table %s. Error: %s",
          hiveTable.getTableName(), ex));
      return;
    }

    // Listing partitions in Hive should always be done after the list on
    // Snowflake. This prevents the edge case where a partition is added
    // between the Hive and Snowflake list operations.
    List<Partition> hivePartitions = hmsClient.listPartitions(
        databaseName, hiveTable.getTableName(), (short) -1 /* all partitions */);
    Preconditions.checkNotNull(hivePartitions);
    log.info(String.format("Found %s partitions in Hive.", hivePartitions.size()));

    // Ensure that no file in Snowflake is removed if it's prefixed by any Hive
    // location, by filtering with a generated regex
    Pattern hivePartitionRegex = Pattern.compile(String.format("(%s).*",
        String.join("|",
                    hivePartitions.stream()
                        .map(partition -> StringUtil.relativizePartitionURI(
                            hiveTable, Preconditions.checkNotNull(partition)))
                        .collect(Collectors.toList()))));
    List<String> extraPartitions = sfPartLocs.stream()
        .filter(location -> hivePartitions.isEmpty()
                            || !hivePartitionRegex.matcher(location).matches())
        .collect(Collectors.toList());

    if (extraPartitions.isEmpty())
    {
      log.info(String.format("No need to drop partitions for table %s",
                             hiveTable.getTableName()));
      return;
    }

    // Drop partitions that aren't in Hive
    log.info(String.format("Dropping %s partition locations",
                           extraPartitions.size()));
    SnowflakeClient.generateAndExecuteSnowflakeStatements(
        new DropPartition(hiveTable, extraPartitions.iterator()),
        snowflakeConf);
  }

  /**
   * Retrieves a set of locations for a Snowflake table that can be used as
   * an argument to the drop partition command.
   * @param hiveTable the Hive table
   * @param schema the schema to use for the jdbc connection
   * @return a set of partition locations
   * @throws SQLException Thrown when there was an error executing a Snowflake
   *                      SQL query (if a Snowflake query must be executed).
   * @throws IllegalStateException thrown when the file paths from Snowflake
   *                               are in an unexpected format
   */
  private Set<String> getSnowflakePartitionLocations(Table hiveTable, String schema)
      throws SQLException, IllegalStateException
  {
    // Get the list of files from Snowflake. Note that this abuses the
    // behavior of Snowflake's drop partition command to unregister
    // individual subdirectories instead of actual partitions.
    ResultSet filePathsResult = SnowflakeClient.executeStatement(
        String.format(
            "SELECT FILE_NAME FROM " +
                "table(information_schema.external_table_files('%s'));",
            StringUtil.escapeSqlText(hiveTable.getTableName())),
        snowflakeConf,
        schema);
    Preconditions.checkNotNull(filePathsResult);
    Preconditions.checkState(filePathsResult.getMetaData().getColumnCount() == 1);

    // The relevant paths have the following form:
    // s3://<bucket>/<padding>/<partition location>/<padding>/<file name>
    // |- Hive table --------|
    // |- Hive partitions ------------------------|
    //                         |- Snowflake partitions -----|
    //               |- Snowflake files --------------------------------|
    // To get the Snowflake partitions from the Snowflake files:
    //  - Append the protocol and bucket
    //  - Remove the file name
    //  - Get the path relative to the Hive table
    String[] hiveTableLocSplit = hiveTable.getSd().getLocation().split("/");
    Preconditions.checkArgument(hiveTableLocSplit.length > 2);
    String prefix = String.join("/",
                                Arrays.asList(hiveTableLocSplit).subList(0, 3));

    List<String> snowflakePartitionLocations = new ArrayList<>();
    while (filePathsResult.next())
    {
      // Note: Must call ResultSet.next() once to move cursor to the first row.
      //       Also, column indices are 1-based.
      String filePath = filePathsResult.getString(1);

      Preconditions.checkState(filePath.contains("/"),
          String.format("No directories to partition on. Path: %s.", filePath));

      String absFilePath = prefix + "/" + filePath.substring(0,
                                                             filePath.lastIndexOf("/"));
      Optional<String> partitionLocation = StringUtil.relativizeURI(
          hiveTable.getSd().getLocation(), absFilePath);
      Preconditions.checkState(partitionLocation.isPresent(),
          String.format("Could not relativize %s with %s",
                        hiveTable.getSd().getLocation(), absFilePath));

      snowflakePartitionLocations.add(partitionLocation.get());
    }
    log.info(String.format("Found %s files in Snowflake.", snowflakePartitionLocations.size()));

    return new HashSet<>(snowflakePartitionLocations);
  }

  /**
   * A convenient entry point for the sync tool. Expects Hive and Hadoop
   * libraries to be in the classpath.
   * See also: {@link #sync()}
   * @param args program arguments, which are not used
   * @throws TException thrown when encountering a Thrift exception while
   *         communicating with the metastore or executing a metastore operation
   */
  public static void main(final String[] args) throws TException
  {
    Preconditions.checkArgument(args.length == 0,
                                "The Hive sync tool expects no arguments.");
    new HiveSyncTool(new HiveMetaStoreClient(new HiveConf())).sync();
  }
}
