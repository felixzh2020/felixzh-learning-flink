import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;

public class KafkaFlinkIdea {
    public static void main(String[] args) throws Exception {

        final ParameterTool para = ParameterTool.fromArgs(args);
        if (!para.has("path")) {
            System.out.println("Error: not exist --path /opt/your.properties");
            System.out.println("Usage: flink run -t yarn-per-job -d /opt/your.jar --path /opt/your.properties");
            System.exit(0);
        }

        ParameterTool param = ParameterTool.fromPropertiesFile(para.getRequired("path"));

        if (param.getRequired("kerberos.enable").equalsIgnoreCase("true") && OperatingSystem.IS_WINDOWS) {
            System.setProperty("java.security.auth.login.config", "d:\\KafkaFlinkIdeaDemoConf//kafka_client_jaas.conf");
            System.setProperty("java.security.krb5.conf", "d:\\KafkaFlinkIdeaDemoConf//krb5.conf");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (param.getRequired("checkpoint.enable").equals("1")) {
            env.getConfig().setUseSnapshotCompression(true);
            env.enableCheckpointing(Long.parseLong(param.getRequired("checkpoint.interval"))); // create a checkpoint
        }

        env.getConfig().setGlobalJobParameters(param); // make parameters available in the web interface

        Properties consumerProp = new Properties();
        consumerProp.setProperty("bootstrap.servers", param.getRequired("consumer.bootstrap.servers"));
        consumerProp.setProperty("group.id", param.getRequired("consumer.group.id"));
        if (param.getRequired("kerberos.enable").equalsIgnoreCase("true")) {
            consumerProp.setProperty("security.protocol", "SASL_PLAINTEXT");
            consumerProp.setProperty("sasl.mechanism", "GSSAPI");
            consumerProp.setProperty("sasl.kerberos.service.name", "kafka");
        }

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(param.getRequired("consumer.kafka.topic"),
                new SimpleStringSchema(), consumerProp);

        DataStream<String> sourceDataStream = env.addSource(kafkaConsumer);
        sourceDataStream.print();

        env.execute();
    }
}
