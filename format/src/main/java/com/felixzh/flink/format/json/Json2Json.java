package com.felixzh.flink.format.json;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author felixzh
 * 微信公众号：大数据从业者
 * 文章地址：https://mp.weixin.qq.com/s/9FSvDuucNHbzg-vL6oRWtw
 * 博客地址：https://www.cnblogs.com/felixzh/
 * */
public class Json2Json {
    private static Logger logger = LoggerFactory.getLogger(Json2Json.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        logger.info(env.getConfig().toString());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceDDL = "CREATE TABLE json_source (" +
                "user_id INT, " +
                "product STRING," +
                "ts timestamp(3)," +
                "watermark for ts as ts - interval '5' second" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'topic' = 'json_source',\n" +
                " 'properties.bootstrap.servers' = 'felixzh:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'json',\n" +
                " 'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'true'\n" +
                ")";

        String sinkDDL = "CREATE TABLE sink (user_id INT,product STRING) WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'sink',\n" +
                " 'properties.bootstrap.servers' = 'felixzh:9092',\n" +
                " 'format' = 'json'\n" +
                ")";

        String transformSQL = "insert into sink(user_id,product) SELECT user_id,product FROM json_source ";

        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);
        tableEnv.executeSql(transformSQL);
    }
}
