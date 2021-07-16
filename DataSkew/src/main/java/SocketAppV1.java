import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketAppV1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> socketData = env.socketTextStream("10.121.198.220", 4444);

        DataStream<Tuple2<String, Integer>> tupleData = socketData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                Tuple2<String, Integer> data = new Tuple2<>();
                data.setFields(value.split(",")[0], Integer.parseInt(value.split(",")[1]));
                out.collect(data);
            }
        }).setParallelism(4);

        tupleData.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                Tuple2<String, Integer> data = new Tuple2<>();
                data.setFields(value1.f0, value1.f1 + value2.f1);
                return data;
            }
        }).print();

        env.execute("SocketApplication");
    }
}
