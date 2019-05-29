#!/bin/bash
set -e

HIVE_CLASSPATH=$(hive -e "set env:CLASSPATH;" | grep env:CLASSPATH= | sed -e "s/^env:CLASSPATH=//")
java -cp $HIVE_CLASSPATH:. com.snowflake.core.util.HiveSyncTool