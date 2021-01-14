import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class KafkaFlinkIdea {
    public static void main(String[] args) throws Exception {

        final ParameterTool para = ParameterTool.fromArgs(args);
        if (!para.has("path")) {
            System.out.println("Error: not exist --path /opt/your.properties");
            System.out.println("Usage: flink run -m yarn-cluster -d /opt/your.jar --path /opt/your.properties");
            System.exit(0);
        }

        if (OperatingSystem.IS_WINDOWS) {
            System.setProperty("java.security.auth.login.config", "d:\\KafkaFlinkIdeaDemoConf//kafka_client_jaas.conf");
            System.setProperty("java.security.krb5.conf", "d:\\KafkaFlinkIdeaDemoConf//krb5.conf");
        }


        ParameterTool param = ParameterTool.fromPropertiesFile(para.getRequired("path"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (param.getRequired("checkpoint.enable").equals("1")) {
            env.getConfig().setUseSnapshotCompression(true);
            env.enableCheckpointing(Long.valueOf(param.getRequired("checkpoint.interval"))); // create a checkpoint every 5 seconds
        }

        env.getConfig().setGlobalJobParameters(param); // make parameters available in the web interface

        Properties consumerProp = new Properties();
        consumerProp.setProperty("bootstrap.servers", param.getRequired("consumer.bootstrap.servers"));
        consumerProp.setProperty("group.id", param.getRequired("consumer.group.id"));
        consumerProp.setProperty("security.protocol", "SASL_PLAINTEXT");
        consumerProp.setProperty("sasl.mechanism", "GSSAPI");
        consumerProp.setProperty("sasl.kerberos.service.name", "kafka");

        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(param.getRequired("consumer.kafka.topic"),
                new SimpleStringSchema(), consumerProp);

        DataStream<String> sourceDataStream = env.addSource(kafkaConsumer);
        sourceDataStream.print();

        env.execute();
    }
}
