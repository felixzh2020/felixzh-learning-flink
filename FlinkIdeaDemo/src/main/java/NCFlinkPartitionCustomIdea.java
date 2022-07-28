import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NCFlinkPartitionCustomIdea {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int maxParallelism = env.getMaxParallelism();
        //env.enableCheckpointing(10_000);
        env.setRestartStrategy(RestartStrategies.noRestart());
        DataStream<String> socketData = env.socketTextStream("felixzh", 4444);
        DataStream<String> map = socketData.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).setParallelism(2);
        //keyby在key分布不均匀的时候，会造成数据倾斜
        //DataStream<String> dataStream = map.keyBy(new MyKeySelector());
        //partitionCustom可以自定义分区方法以规避数据倾斜
        DataStream<String> dataStream = map.partitionCustom(new MyPartitioner(), new MyKeySelector());
        dataStream.print().setParallelism(3);

        env.execute();
    }
}

class MyPartitioner implements Partitioner<String> {

    //numPartitions为下个算子的并发度.
    //通过partition方法
    @Override
    public int partition(String key, int numPartitions) {
        System.out.println(String.format("numPartitions: %s", numPartitions));
        int index = key.hashCode() % numPartitions;
        System.out.println(String.format("index: %s", index));
        return index;
    }

}


class MyKeySelector implements KeySelector<String, String> {
    //从每条数据中选出key值：单个字段或者多个字段的组合
    @Override
    public String getKey(String value) throws Exception {
        return value;
    }
}
