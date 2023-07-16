package behavior;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

    @Override
    public void apply(Long key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
        collector.collect(new ItemViewCount(key, timeWindow.getEnd(), iterable.iterator().next()));
    }
}
