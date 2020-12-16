package com.felixzh.flink.format.avro;

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
public class Avro2Avro {
    private static Logger logger = LoggerFactory.getLogger(Avro2Avro.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        logger.info(env.getConfig().toString());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceDDL = "CREATE TABLE avro_source (id INT, name STRING) WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'scan.startup.mode' = 'earliest-offset',\n" +
                " 'topic' = 'avro_source1',\n" +
                " 'properties.bootstrap.servers' = 'felixzh:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'avro',\n" +
                " 'property-version' = '1',\n" +
                /*" 'avro-schema' = '{\"type\":\"record\",\"name\":\"Customer\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}'\n" +
               */ ")";

        String sinkDDL = "CREATE TABLE avro_sink (id INT,name STRING) WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'avro_sink',\n" +
                " 'properties.bootstrap.servers' = 'felixzh:9092',\n" +
                " 'format' = 'avro'\n" +
                ")";

        String transformSQL = "INSERT INTO avro_sink SELECT id,name FROM avro_source ";

        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);
        tableEnv.executeSql(transformSQL).print();
    }
}
