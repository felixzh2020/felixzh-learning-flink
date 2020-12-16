package com.felixzh.flink.format.canal_json;

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
public class Kafka2Print {
    Logger logger = LoggerFactory.getLogger(Kafka2Print.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                "CREATE TABLE topic_products (\n" +
                        "  id BIGINT,\n" +
                        "  name STRING,\n" +
                        "  description STRING,\n" +
                        "  weight DECIMAL(10, 2)\n" +
                        ") WITH (\n" +
                        " 'connector' = 'kafka',\n" +
                        " 'scan.startup.mode' = 'earliest-offset',\n" +
                        " 'topic' = 'products_binlog',\n" +
                        " 'properties.bootstrap.servers' = 'felixzh:9092',\n" +
                        " 'properties.group.id' = 'testGroup',\n" +
                        " 'canal-json.ignore-parse-errors' = 'true',\n" +
                        " 'format' = 'canal-json'\n" +
                        ")";

        String sinkDDL =
                "CREATE TABLE tb_sink (\n" +
                        " id BIGINT,\n" +
                        " name STRING,\n" +
                        " description STRING,\n" +
                        " weight DECIMAL(10, 2)\n" +
                        ") WITH (\n" +
                        " 'connector' = 'print'\n" +
                        ")";

        String transformSQL =
                "INSERT INTO tb_sink " +
                        "SELECT * " +
                        "FROM topic_products ";

        tableEnv.executeSql(sourceDDL).print();
        tableEnv.executeSql(sinkDDL).print();
        tableEnv.executeSql(transformSQL).print();
    }
}

/**
 1. 向kafka发送数据，模拟使用canal将binlog发送到kafka
 可以直接使用https://github.com/felixzh2020/felixzh-learning-kafka/blob/master/src/main/java/org/felixzh/kafka/string/StringProducer.java
 {
 "data": [
 {
 "id": "111",
 "name": "scooter",
 "description": "Big 2-wheel scooter",
 "weight": "5.18"
 }
 ],
 "database": "inventory",
 "es": 1589373560000,
 "id": 9,
 "isDdl": false,
 "mysqlType": {
 "id": "INTEGER",
 "name": "VARCHAR(255)",
 "description": "VARCHAR(512)",
 "weight": "FLOAT"
 },
 "old": [
 {
 "weight": "5.15"
 }
 ],
 "pkNames": [
 "id"
 ],
 "sql": "",
 "sqlType": {
 "id": 4,
 "name": 12,
 "description": 12,
 "weight": 7
 },
 "table": "products",
 "ts": 1589373560798,
 "type": "UPDATE"
 }

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
 10:45:00,654 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - Log file environment variable 'log.file' is not set.
 10:45:00,666 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - JobManager log files are unavailable in the web dashboard. Log file location not found in environment variable 'log.file' or configuration key 'web.log.path'.
 +------------------------------------------+
 | default_catalog.default_database.tb_sink |
 +------------------------------------------+
 |                                       -1 |
 +------------------------------------------+
 1 row in set
 10:45:02,604 WARN  org.apache.flink.runtime.taskmanager.TaskManagerLocation      - No hostname could be resolved for the IP address 127.0.0.1, using IP address as host name. Local input split assignment (such as for HDFS files) may be impacted.
 10:45:02,884 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name Sink: Sink(table=[default_catalog.default_database.tb_sink], fields=[id, name, description, weight]) exceeded the 80 characters length limit and was truncated.
 10:45:02,920 WARN  org.apache.flink.metrics.MetricGroup                          - The operator name Source: TableSourceScan(table=[[default_catalog, default_database, topic_products]], fields=[id, name, description, weight]) exceeded the 80 characters length limit and was truncated.
 -U(111,scooter,Big 2-wheel scooter,5.15)
 +U(111,scooter,Big 2-wheel scooter,5.18)
 * */
