Snowflake Hive SDK
******************

Usage:
======

1. Add a Snowflake configuration file named 'snowflake-config.xml' in hive/conf. The file should be in the form

.. code-block:: bash

    <configuration>
      <property>
        <name>snowflake.jdbc.username</name>
        <value>mySnowflakeUsername</value>
      </property>
    </configuration>

The properties required are:
snowflake.jdbc.username
snowflake.jdbc.password
snowflake.jdbc.account
snowflake.jdbc.db
snowflake.jdbc.schema
snowflake.jdbc.connection

For more information on the Snowflake JDBC driver, see: https://docs.snowflake.net/manuals/user-guide/jdbc.html

2. Package the jar by running:

.. code-block:: bash

    mvn package

3. Copy the jar into the Hive classpath

4. Update the Hive metastore configuration to point to the listener. To do this, simply add the following section to hive-site.xml (typically found in hive/conf):

.. code-block:: bash
    <configuration>
      ...
      <property>
        <name>hive.metastore.event.listeners</name>
        <value>com.snowflake.hive.listener.SnowflakeHiveListener</value>
      </property>
    </configuration>
    
5. Restart the Hive metastore.
