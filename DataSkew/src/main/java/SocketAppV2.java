import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Random;

public class SocketAppV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> socketData = env.socketTextStream("10.121.198.220", 4444);

        Random random = new Random(1);
        DataStream<Tuple2<String, Integer>> tupleData = socketData.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                Tuple2<String, Integer> data = new Tuple2<>();
                data.setFields(value.split(",")[0] + "_" + random.nextInt(50), Integer.parseInt(value.split(",")[1]));
                out.collect(data);
            }
        }).setParallelism(4);

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tupleData.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        keyedStream.print("增加后缀标签打散key");

        SingleOutputStreamOperator<HashMap<String, Integer>> dataStream1 = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5))).aggregate(new MyCountAggregate()).setParallelism(4);
        dataStream1.print("第一次keyBy输出");

        DataStream<Tuple2<String, Integer>> dataStream2 = dataStream1.flatMap(new FlatMapFunction<HashMap<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(HashMap<String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
                value.forEach((k, v) -> {
                    Tuple2<String, Integer> data = new Tuple2<>();
                    data.setFields(k.split("_")[0], v);
                    out.collect(data);
                });
            }
        }).setParallelism(4);

        dataStream2.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                Tuple2<String, Integer> data = new Tuple2<>();
                data.setFields(value1.f0, value1.f1 + value2.f1);
                return data;
            }
        }).setParallelism(4).print("第二次keyBy输出");

        env.execute("SocketApplicationV2");
    }
}
