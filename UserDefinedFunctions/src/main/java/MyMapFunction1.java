import org.apache.flink.api.common.functions.MapFunction;

import java.util.Date;

public class MyMapFunction1 implements MapFunction<String, String> {

    @Override
    public String map(String s) throws Exception {
        return s + " " + new Date();
    }
}
