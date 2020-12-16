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
 * */
public class MySqlCDC2Print {
    Logger logger = LoggerFactory.getLogger(MySqlCDC2Print.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                "CREATE TABLE mysql_binlog (\n" +
                        " id INT NOT NULL,\n" +
                        " name STRING,\n" +
                        " description STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'mysql-cdc',\n" +
                        " 'hostname' = 'felixzh',\n" +
                        " 'port' = '3306',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = 'passwd',\n" +
                        " 'database-name' = 'db_felixzh_cdc',\n" +
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
                        "FROM mysql_binlog ";

        tableEnv.executeSql(sourceDDL).print();
        tableEnv.executeSql(sinkDDL).print();
        tableEnv.executeSql(transformSQL).print();

    }
}

/***
 1. 准备库表
 mysql> CREATE DATABASE db_felixzh_cdc;
 Query OK, 1 row affected (0.00 sec)

 mysql> CREATE TABLE tb_products_cdc( id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), description VARCHAR(128) );
 Query OK, 0 rows affected (0.01 sec)

 mysql> INSERT INTO tb_products_cdc VALUES  (DEFAULT, 'zhangsan', 'aaa'), (DEFAULT, 'lisi', 'bbb'), (DEFAULT, 'wangwu', 'ccc');
 Query OK, 3 rows affected (0.01 sec)
 Records: 3  Duplicates: 0  Warnings: 0

 2. idea(IDE)执行结果
 +--------+
 | result |
 +--------+
 |     OK |
 +--------+
 1 row in set
 +--------+
 | result |
 +--------+
 0 row in set
 14:50:02,488 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - Log file environment variable 'log.file' is not set.
 14:50:02,495 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - JobManager log files are unavailable in the web dashboard. Log file location not found in environment variable 'log.file' or configuration key 'web.log.path'.
 +------------------------------------------+
 | default_catalog.default_database.tb_sink |
 +------------------------------------------+
 |                                       -1 |
 +------------------------------------------+
 1 row in set
 14:50:04,432 WARN  org.apache.flink.runtime.taskmanager.TaskManagerLocation      - No hostname could be resolved for the IP address 127.0.0.1, using IP address as host name. Local input split assignment (such as for HDFS files) may be impacted.
 14:50:04,688 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name Sink: Sink(table=[default_catalog.default_database.tb_sink], fields=[id, name, description]) exceeded the 80 characters length limit and was truncated.
 14:50:04,711 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name Source: TableSourceScan(table=[[default_catalog, default_database, mysql_binlog]], fields=[id, name, description]) exceeded the 80 characters length limit and was truncated.
 +I(1,zhangsan,aaa)
 +I(2,wangwu,bbb)
 +I(3,wangwu,ccc)
 +I(5,lisi,ddd)
 十二月 04, 2020 2:50:09 下午 com.github.shyiko.mysql.binlog.BinaryLogClient connect
 信息: Connected to felixzh:3306 at mysql-bin.000003/1347 (sid:5585, cid:10)
 +I(6,lisi,ddd)
 -D(1,zhangsan,aaa)
 * */
