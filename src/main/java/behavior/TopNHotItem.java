package behavior;


import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class TopNHotItem extends KeyedProcessFunction<Long, ItemViewCount, String> {
    private ListState<ItemViewCount> listState;
    private Integer top;

    public TopNHotItem(Integer top) {
        this.top = top;
    }

    @Override
    public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
        listState.add(itemViewCount);
        context.timerService().registerEventTimeTimer(itemViewCount.getWindow() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        Iterable<ItemViewCount> itemViewCounts = listState.get();
        List<ItemViewCount> list = IteratorUtils.toList(itemViewCounts.iterator());
        List<ItemViewCount> collect = list.stream().sorted(Comparator.comparing(ItemViewCount::getCount).reversed()).collect(Collectors.toList());
        listState.clear();

        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("Timestamp: ").append(new Timestamp(timestamp - 1)).append("\n");
        for(int i = 0; i < top; i++){
            ItemViewCount itemViewCount = collect.get(i);
            stringBuffer.append("number").append(i + 1).append(": ")
                    .append(" ItemId=").append(itemViewCount.getItemId())
                    .append(" ViewCount=").append(itemViewCount.getCount())
                    .append("\n");
        }

        stringBuffer.append("===================================");
        Thread.sleep(1000);
        out.collect(stringBuffer.toString());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item", ItemViewCount.class));
    }
}
