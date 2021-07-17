import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class MyCountAggregate implements AggregateFunction<Tuple2<String, Integer>, HashMap<String, Integer>, HashMap<String, Integer>> {
    @Override
    public HashMap<String, Integer> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<String, Integer> add(Tuple2<String, Integer> value, HashMap<String, Integer> accumulator) {
        accumulator.merge(value.f0, value.f1, Integer::sum);
        /**
         * if (accumulator.get(value.f0) != null) {
         *             accumulator.put(value.f0, value.f1 + accumulator.get(value.f0));
         *         } else {
         *             accumulator.put(value.f0, value.f1);
         *         }
         *
         * */
        return accumulator;
    }

    @Override
    public HashMap<String, Integer> getResult(HashMap<String, Integer> accumulator) {
        return accumulator;
    }

    @Override
    public HashMap<String, Integer> merge(HashMap<String, Integer> a, HashMap<String, Integer> b) {
        for (Map.Entry<String, Integer> entry : a.entrySet()) {
            b.merge(entry.getKey(), entry.getValue(), Integer::sum);
            /**
             *            if(b.get(entry.getKey()) == null){
             *                 b.put(entry.getKey(), entry.getValue());
             *             }else{
             *                 b.put(entry.getKey(), entry.getValue() + b.get(entry.getKey()));
             *             }
             * */
        }
        return b;
    }



/*    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Tuple2<String, Integer> value, Long accumulator) {

        return accumulator + value.f1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }*/
}
