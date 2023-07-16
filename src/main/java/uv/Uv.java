package uv;

import behavior.UserBehavior;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pageview.PageView;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class Uv {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile(Uv.class.getResource("/UserBehavior.csv").getPath())
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
                        String[] splits = value.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                Long.valueOf(splits[0]),
                                Long.valueOf(splits[1]),
                                Integer.valueOf(splits[2]),
                                splits[3].trim(),
                                Long.valueOf(splits[4])
                        );
                        out.collect(userBehavior);
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior behavior, long recordTimeStamp) {
                        return behavior.getTimeStamp() * 1000;
                    }
                })).filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior behavior) throws Exception {
                        return behavior.getBehavior().equals("pv");
                    }
                }).timeWindowAll(Time.hours(1)).apply(new AllWindowFunction<UserBehavior, UvCount, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<UvCount> collector) throws Exception {
                        Set<Long> set = new LinkedHashSet<>();
                        List<UserBehavior> list = IteratorUtils.toList(values.iterator());
                        for(UserBehavior user : list){
                            set.add(user.getUserId());
                        }
                        collector.collect(new UvCount(window.getEnd(), set.size()));
                    }
                }).print();
        env.execute();
    }
}
