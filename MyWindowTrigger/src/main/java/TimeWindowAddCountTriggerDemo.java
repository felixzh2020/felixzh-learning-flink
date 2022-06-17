import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class TimeWindowAddCountTriggerDemo {
    public static void main(String... args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> socket = env.socketTextStream("felixzh", 4444, "\n");
        //Flink内置CountTrigger，直接拿来用
        //其实就是CountWindowAll里面使用的Trigger触发器
        /**
         *  public AllWindowedStream<T, GlobalWindow> countWindowAll(long size) {
         *         return windowAll(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(size)));
         *     }
         * */
        CountTrigger<TimeWindow> countTrigger = CountTrigger.of(5);
        socket.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .trigger(countTrigger)
                //.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<String, Object, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> elements, Collector<Object> out) throws Exception {
                        elements.forEach(value -> out.collect(value + " " + new Date()));
                    }
                })
                .print();

        env.execute();
    }
}
