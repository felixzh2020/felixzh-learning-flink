import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyCountTrigger<W extends Window> extends Trigger<Object, TimeWindow> {

    private static Logger logger = LoggerFactory.getLogger(MyCountTrigger.class);

    //触发条数
    private long maxCount;

    //开启事件时间
    private boolean eventTimeEnable = false;

    private ReducingStateDescriptor<Long> countStateDescriptor = new ReducingStateDescriptor<Long>
            ("counter-name", new Sum(), LongSerializer.INSTANCE);

    public MyCountTrigger(long maxCount) {
        this.maxCount = maxCount;
    }

    public MyCountTrigger(long maxCount, boolean eventTimeEnable) {
        this.maxCount = maxCount;
        this.eventTimeEnable = eventTimeEnable;
    }


    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> count = ctx.getPartitionedState(countStateDescriptor);
        count.add(1L);
        //定时定量触发
        if (count.get() >= maxCount || timestamp >= window.getEnd()) {
            count.clear();
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (eventTimeEnable || time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        }

        ctx.getPartitionedState(countStateDescriptor).clear();
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (!eventTimeEnable || time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        }

        ctx.getPartitionedState(countStateDescriptor).clear();
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        //清除状态数据
        ctx.getPartitionedState(countStateDescriptor).clear();
    }

    //创建MyCountTrigger实例
    public static <W extends Window> MyCountTrigger<W> of(long count) {
        return new MyCountTrigger<>(count);
    }

    //创建MyCountTrigger实例
    public static <W extends Window> MyCountTrigger<W> of(long count, boolean eventTimeEnable) {
        return new MyCountTrigger<>(count, eventTimeEnable);
    }

    //增量计数方法
    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }
}
