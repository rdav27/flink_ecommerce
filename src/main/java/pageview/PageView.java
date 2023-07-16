package pageview;

import behavior.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PageView {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile(PageView.class.getResource("/UserBehavior.csv").getPath())
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
                    public long extractTimestamp(UserBehavior behavior, long recordTimestamp) {
                        return behavior.getTimeStamp() * 1000;
                    }
                })).filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior behavior) throws Exception {
                        return behavior.getBehavior().equals("pv");
                    }
                }).map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBehavior userBehavior) throws Exception {
                        return Tuple2.of(userBehavior.getBehavior(), 1);
                    }
                }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                }).timeWindow(Time.hours(1)).sum(1).print();
        env.execute();
    }
}
