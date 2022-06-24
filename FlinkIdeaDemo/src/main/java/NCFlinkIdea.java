import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NCFlinkIdea {
    public static void main(String... args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> socketData = env.socketTextStream("felixzh", 4444);

        //方法1：通过反射newInstance实例化对象。优点：解耦、可配置类名称
        String mapFunctionClassName = "MyMapFunction1";
        //String mapFunctionClassName = "MyMapFunction";
        Class<?> myMapFunction = Class.forName(mapFunctionClassName);
        Object object = myMapFunction.newInstance();
        socketData.map((MapFunction<String, Object>) object).print();

        //方法2：通过new实例化对象。缺点：耦合固定类名称
        //socketData.map(new MyMapFunction()).print();
        //socketData.map(new MyMapFunction1()).print();

        env.execute();
    }
}
