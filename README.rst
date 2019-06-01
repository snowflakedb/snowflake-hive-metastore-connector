Snowflake Hive Metastore Connector
**********************************

The Snowflake Hive metastore connector provides an easy way to query Hive-managed data via Snowflake. Once installed, the connector listens to Hive metastore events and creates the equivalent Snowflake objects.

Installation:
=============

#. Create a new file named 'snowflake-config.xml' to the same directory that contains hive-site.xml. This will be the configuration file for the connector. This configuration file should look like:

   .. code-block:: xml
 
     <configuration>
       <property>
         <name>snowflake.jdbc.username</name>
         <value>...</value>
       </property>
       <property>
         <name>snowflake.jdbc.password</name>
         <value>...</value>
       </property>
       <property>
         <name>snowflake.jdbc.role</name>
         <value>...</value>
       </property>
       <property>
         <name>snowflake.jdbc.account</name>
         <value>...</value>
       </property>
       <property>
         <name>snowflake.jdbc.db</name>
         <value>...</value>
       </property>
       <property>
         <name>snowflake.jdbc.schema</name>
         <value>...</value>
       </property>
       <property>
         <name>snowflake.jdbc.connection</name>
         <value>jdbc:snowflake://account.snowflakecomputing.com</value>
       </property>
       <property>
         <name>snowflake.hive-metastore-listener.integration</name>
         <value>...</value>
       </property>
     </configuration>

#. Package the jar by running:

   .. code-block:: bash

     mvn package

#. Copy the jar into the Hive classpath

#. Update the Hive metastore configuration to point to the listener. To do this, simply add the following section to hive-site.xml:

   .. code-block:: xml

     <configuration>
       ...
       <property>
         <name>hive.metastore.event.listeners</name>
         <value>net.snowflake.hivemetastoreconnector.SnowflakeHiveListener</value>
       </property>
     </configuration>
    
#. Restart the Hive metastore.

Usage:
======

#. In Hive, touch partitions to be used in Snowflake:

   .. code-block::

     alter table <table_name> touch partition <partition_spec>;

   Or for non-partitioned Hive tables:

   .. code-block::

     alter table <table_name> touch;

#. Query the table in Snowflake:

   .. code-block::

     select * from <table_name>;
