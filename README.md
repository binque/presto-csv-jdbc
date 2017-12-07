# Presto CSV JDBC Plugin

This is a plugin for Presto that allow you to use Hive JDBC Connection

## Connection Configuration

Create new properties file inside etc/catalog dir:

* connector.name=hive-jdbc
* connection-url=jdbc:relique:csv:/path

Create a dir inside plugin dir called csv-jdbc. To make it easier you could copy all the libs from the target/lib/*.jars. to /plugin/csv-jdbc

## Building Presto CSV JDBC Plugin

mvn clean install
