package behavior;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Hotitem {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.readTextFile(Hotitem.class.getResource("/UserBehavior.csv").getPath()).flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String value, Collector<UserBehavior> collector) throws Exception {
                String[] splits = value.split(",");
                UserBehavior behavior = new UserBehavior(
                        Long.valueOf(splits[0]),
                        Long.valueOf(splits[1]),
                        Integer.valueOf(splits[2]),
                        splits[3].trim(),
                        Long.valueOf(splits[4])
                );
                collector.collect(behavior);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior behavior, long recordTimestamp) {
                return behavior.getTimeStamp() * 1000;
            }
        })).filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior behavior) throws Exception {
                return behavior.getBehavior().equals("pv");
            }
        }).keyBy(new KeySelector<UserBehavior, Long>() {
            @Override
            public Long getKey(UserBehavior behavior) throws Exception {
                return behavior.getItemId();
            }
        }).timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(new KeySelector<ItemViewCount, Long>() {
                    @Override
                    public Long getKey(ItemViewCount itemViewCount) throws Exception {
                        return itemViewCount.getWindow();
                    }
                }).process(new TopNHotItem(3)).print();
        env.execute();
    }
}
