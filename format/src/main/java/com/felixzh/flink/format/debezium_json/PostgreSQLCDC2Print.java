package com.felixzh.flink.format.debezium_json;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author felixzh
 * 微信公众号：大数据从业者
 * 博客地址：https://www.cnblogs.com/felixzh/
 */
public class PostgreSQLCDC2Print {
    Logger logger = LoggerFactory.getLogger(PostgreSQLCDC2Print.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                "CREATE TABLE postgresql_log (\n" +
                        " id INT NOT NULL,\n" +
                        " name STRING,\n" +
                        " description STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'postgres-cdc',\n" +
                        " 'hostname' = 'felixzh',\n" +
                        " 'port' = '5432',\n" +
                        " 'username' = 'postgres',\n" +
                        " 'password' = 'passwd',\n" +
                        " 'database-name' = 'db_felixzh_cdc',\n" +
                        " 'schema-name' = 'db_felixzh_cdc',\n" +
                        " 'table-name' = 'tb_products_cdc'\n" +
                        ")";

        String sinkDDL =
                "CREATE TABLE tb_sink (\n" +
                        " id INT,\n" +
                        " name STRING,\n" +
                        " description STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'print'\n" +
                        ")";

        String transformSQL =
                "INSERT INTO tb_sink " +
                        "SELECT * " +
                        "FROM postgresql_log ";

        tableEnv.executeSql(sourceDDL).print();
        tableEnv.executeSql(sinkDDL).print();
        tableEnv.executeSql(transformSQL).print();

    }
}