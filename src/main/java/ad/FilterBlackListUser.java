package ad;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FilterBlackListUser extends KeyedProcessFunction<String, AdClick, AdClick> {
    private ValueState<Long> countState;
    private ValueState<Boolean> isSendState;
    private ValueState<Long> timeState;
    private Integer maxCount;
    private OutputTag<BlackWarning> outputTag;
    public FilterBlackListUser(Integer maxCount, OutputTag<BlackWarning> outputTag){
        this.maxCount = maxCount;
        this.outputTag = outputTag;
    }

    @Override
    public void processElement(AdClick adClick, Context ctx, Collector<AdClick> collector) throws Exception {
        Long count = this.countState.value();
        if(count == null){
            count = 0L;
            isSendState.update(false);
        }
        if(count == 0){
            Long ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24);
        }
        if(count >= maxCount){
            if(isSendState.value() != null && !isSendState.value()){
                isSendState.update(true);
                ctx.output(outputTag, new BlackWarning(adClick.getUserId(), adClick.getAdId(), " click is above" + maxCount));
            }
            return;
        }
        countState.update(count + 1);
        collector.collect(adClick);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClick> out) throws Exception {
        if(timestamp == timeState.value()){
            isSendState.clear();
            countState.clear();
            timeState.clear();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
        isSendState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("issend", Boolean.class));
        timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time", Long.class));
    }
}
