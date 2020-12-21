package com.felixzh.flink.format.canal_json;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author felixzh
 * 微信公众号：大数据从业者
 * 文章地址：https://mp.weixin.qq.com/s/nWAkyThD2d7rQxBcKXRRLw
 * 博客地址：https://www.cnblogs.com/felixzh/
 * */
public class MySqlCDC2Kafka {
    Logger logger = LoggerFactory.getLogger(MySqlCDC2Kafka.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceDDL =
                "CREATE TABLE mysql_binlog (\n" +
                        " id  INT NOT NULL,\n" +
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
                        "  id BIGINT,\n" +
                        "  name STRING,\n" +
                        "  description STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'kafka',\n" +
                        " 'scan.startup.mode' = 'earliest-offset',\n" +
                        " 'topic' = 'mysql_binlog',\n" +
                        " 'properties.bootstrap.servers' = 'felixzh:9092',\n" +
                        " 'properties.group.id' = 'testGroup',\n" +
                        " 'canal-json.ignore-parse-errors' = 'true',\n" +
                        " 'format' = 'canal-json'\n" +
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
 Exception in thread "main" org.apache.flink.table.api.ValidationException: Unable to create a sink for writing table 'default_catalog.default_database.tb_sink'.

 Table options are:

 'canal-json.ignore-parse-errors'='true'
 'connector'='kafka'
 'format'='canal-json'
 'properties.bootstrap.servers'='felixzh:9092'
 'properties.group.id'='testGroup'
 'scan.startup.mode'='earliest-offset'
 'topic'='mysql_binlog'
 at org.apache.flink.table.factories.FactoryUtil.createTableSink(FactoryUtil.java:164)
 at org.apache.flink.table.planner.delegation.PlannerBase.getTableSink(PlannerBase.scala:344)
 at org.apache.flink.table.planner.delegation.PlannerBase.translateToRel(PlannerBase.scala:204)
 at org.apache.flink.table.planner.delegation.PlannerBase$$anonfun$1.apply(PlannerBase.scala:163)
 at org.apache.flink.table.planner.delegation.PlannerBase$$anonfun$1.apply(PlannerBase.scala:163)
 at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
 at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
 at scala.collection.Iterator$class.foreach(Iterator.scala:891)
 at scala.collection.AbstractIterator.foreach(Iterator.scala:1334)
 at scala.collection.IterableLike$class.foreach(IterableLike.scala:72)
 at scala.collection.AbstractIterable.foreach(Iterable.scala:54)
 at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
 at scala.collection.AbstractTraversable.map(Traversable.scala:104)
 at org.apache.flink.table.planner.delegation.PlannerBase.translate(PlannerBase.scala:163)
 at org.apache.flink.table.api.internal.TableEnvironmentImpl.translate(TableEnvironmentImpl.java:1264)
 at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeInternal(TableEnvironmentImpl.java:700)
 at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeOperation(TableEnvironmentImpl.java:787)
 at org.apache.flink.table.api.internal.TableEnvironmentImpl.executeSql(TableEnvironmentImpl.java:690)
 at com.felixzh.learning.flink.format.canal_json.MySqlCDC2Kafka.main(MySqlCDC2Kafka.java:55)
 Caused by: org.apache.flink.table.api.ValidationException: Error creating sink format 'canal-json' in option space 'canal-json.'.
 at org.apache.flink.table.factories.FactoryUtil$TableFactoryHelper.lambda$discoverOptionalEncodingFormat$3(FactoryUtil.java:468)
 at java.util.Optional.map(Optional.java:215)
 at org.apache.flink.table.factories.FactoryUtil$TableFactoryHelper.discoverOptionalEncodingFormat(FactoryUtil.java:462)
 at org.apache.flink.table.factories.FactoryUtil$TableFactoryHelper.discoverEncodingFormat(FactoryUtil.java:448)
 at org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactoryBase.createDynamicTableSink(KafkaDynamicTableFactoryBase.java:100)
 at org.apache.flink.table.factories.FactoryUtil.createTableSink(FactoryUtil.java:161)
 ... 18 more
 Caused by: java.lang.UnsupportedOperationException: Canal format doesn't support as a sink format yet.
 at org.apache.flink.formats.json.canal.CanalJsonFormatFactory.createEncodingFormat(CanalJsonFormatFactory.java:95)
 at org.apache.flink.table.factories.FactoryUtil$TableFactoryHelper.lambda$discoverOptionalEncodingFormat$3(FactoryUtil.java:465)
 ... 23 more
 * */