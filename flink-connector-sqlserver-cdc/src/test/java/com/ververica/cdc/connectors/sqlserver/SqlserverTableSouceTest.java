package com.ververica.cdc.connectors.sqlserver;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author zhuchengwen
 * @date 2021/5/12
 */
public class SqlserverTableSouceTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        System.out.println(CREATE_SQL);
        System.out.println(INSERT);

        tableEnvironment.executeSql(CREATE_SQL);
        tableEnvironment.executeSql(CREATE_PRINT);
        tableEnvironment.executeSql(INSERT);
    }

    public static final String CREATE_SQL = "create table customer_CustomerBO (\n" +
            "id int  ,\n" +
            "name string,\n" +
            "age int ,\n" +
            "area int" +
            ") with ( \n" +
            "'connector'='sqlserver-cdc',\n" +
            "'hostname'='xxxxx',\n" +
            "'port'='1433',\n" +
            "'username'='SA',\n" +
            "'password'='123456',\n" +
            "'database-name'='TestDB',\n" +
//            "'schema-name'='dbo',\n" +
            "'table-name'='student'\n" +
            ")" ;


    public static final String CREATE_PRINT = "create table dim_customer_customerbo_delta_sync_raw (\n" +
            "id int  ,\n" +
            "name string,\n" +
            "age int ,\n" +
            "area int " +
            ") with ( " +
            "'connector'='print')";

    public static final String INSERT = "insert into dim_customer_customerbo_delta_sync_raw\n" +
            "select " +
            "`id`,\n" +
            "`name`,\n" +
            "`age`,\n" +
            "`area`\n" +
            "from customer_CustomerBO";
}

