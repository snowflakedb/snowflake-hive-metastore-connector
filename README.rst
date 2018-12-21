Snowflake Hive SDK
******************

Usage:
======

1. Add a Snowflake configuration file.
The file should be in the form

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

For more information on the snowflake jdbc driver, see: https://docs.snowflake.net/manuals/user-guide/jdbc.html

2. Package the jar by running:

.. code-block:: bash

    mvn package

3. Copy the jar into the hive classpath
