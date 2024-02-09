#! /bin/bash

## Description
## This script is meant to run as a custom script action on a Linux HDInsight
## cluster to set up the Snowflake Hive Metastore connector. It downloads the
## Snowflake connector JAR and the Snowflake configuration XML file, and places
## them in the target directories.

## Usage
## This script takes two parameters:
## 1. The wasb url to the snowflake hive connector jar
## 2. The wasb url to the snowflake config file
## These urls are in the following format:
## wasbs://containername@storageaccountname.blob.core.windows.net/pathtofile
## Please review and edit the target directories as they will vary based on
## your cluster.

# Where the JAR should go (WILL VARY BY USER -- please edit)
CUSTOMHIVELIBSDIR=/usr/hdp/2.6.5.3032-3/atlas/hook/hive
# Where the snowflake-config.xml should go (WILL VARY BY USER -- please edit)
SNOWFLAKECONFDIR=/usr/hdp/current/hive-server2/conf/conf.server

# Check paths exist
if [ -e $CUSTOMHIVELIBSDIR ]; then
  echo "$CUSTOMHIVELIBSDIR exists"
else
  echo "Creating $CUSTOMHIVELIBSDIR"
  mkdir $CUSTOMHIVELIBSDIR/
fi

if [ -e $SNOWFLAKECONFDIR ]; then
  echo "$SNOWFLAKECONFDIR exists"
else
  echo "Creating $SNOWFLAKECONFDIR"
  mkdir $SNOWFLAKECONFDIR/
fi

# Download JAR
echo "Downloading Snowflake connector JAR"
echo $1
hadoop fs -copyToLocal $1 $CUSTOMHIVELIBSDIR/snowflake-hive-metastore-connector.jar

# Download snowflake-config.xml
echo "Downloading Snowflake connector configuration file"
echo $2
hadoop fs -copyToLocal $2 $SNOWFLAKECONFDIR/snowflake-config.xml

echo "Setup for Snowflake connector completed!"
