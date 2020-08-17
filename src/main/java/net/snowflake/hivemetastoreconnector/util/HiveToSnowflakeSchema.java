package net.snowflake.hivemetastoreconnector.util;

import com.google.common.base.Preconditions;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A util to get the correct Snowflake schema from the Hive schema.
 */
public class HiveToSnowflakeSchema {
    private static final Logger log = LoggerFactory.getLogger(
            HiveToSnowflakeSchema.class);

    public static String getSnowflakeDefaultSchema(SnowflakeConf snowflakeConf) {
        return snowflakeConf.get(SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_SCHEMA.getVarname());
    }

    public static Set<String> getSnowflakeSchemaSet(SnowflakeConf snowflakeConf) {
        return new HashSet<>(snowflakeConf.getStringCollection(
                SnowflakeConf.ConfVars.SNOWFLAKE_SCHEMA_LIST.getVarname())
                .stream().map(String::toLowerCase).collect(Collectors.toSet()));
    }

    public static String getSnowflakeSchemaFromHiveSchema(
        String hiveSchema,
        String snowflakeDefaultSchema,
        Set<String> snowflakeSchemaSet) {
        Preconditions.checkNotNull(
             snowflakeDefaultSchema,
            "Could not find default Snowflake schema. " +
             "Ensure snowflake-config.xml, contains the " +
             "snowflake.jdbc.schema property.");

        if (snowflakeSchemaSet.contains(hiveSchema.toLowerCase())) {
            log.info(hiveSchema + " is in the configured schema list.");
            return hiveSchema;
        } else {
            log.info(hiveSchema + " is not in the configured schema list. " +
                    "Use default schema:" + snowflakeDefaultSchema);
            return snowflakeDefaultSchema;
        }
    }

    public static String getSnowflakeSchemaFromHiveSchema(String hiveSchema, SnowflakeConf snowflakeConf) {
        return getSnowflakeSchemaFromHiveSchema(
                hiveSchema,
                getSnowflakeDefaultSchema(snowflakeConf),
                getSnowflakeSchemaSet(snowflakeConf));
    }
}
