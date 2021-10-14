# Flink CDC Connectors
Fork from https://github.com/ververica/flink-cdc-connectors
Add flink-connector-sqlserver

## Features
1. Support sqlserver cdc  


```sql
-- creates a sqlserver cdc table source
create table user_table_source (
id int,
age int ,
area String
 ) with (
'connector'='sqlserver-cdc',
'hostname'='xx.xx.xx.xx',
'port'='1433',
'username'='xx',
'password'='xxx',
'database-name'='dbName',
-- 'snapshot-mode'='schema_only',
'table-name'='tableName'
)

```
The above examples are all required parameters, and the non required parameters are as follows：
snapshot-mode： default：initial, also can use schema_only 
schema-name:  default:dbo


## Usage for DataStream API

Include following Maven dependency (available through Maven Central):

```
<dependency>
  <groupId>com.ververica</groupId>
  <!-- add the dependency matching your database -->
  <artifactId>flink-connector-sqlserver-cdc</artifactId>
  <version>2.1.0</version>
</dependency>
```
