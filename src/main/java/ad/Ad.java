package ad;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Ad {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        OutputTag<BlackWarning> blackWarningOutputTag = new OutputTag<BlackWarning>("blacklist"){};

        DataStream<AdClick> clickDataSource = env.readTextFile(Ad.class.getResource("/AdClicking.csv").getPath())
                .flatMap(new FlatMapFunction<String, AdClick>() {
                    @Override
                    public void flatMap(String s, Collector<AdClick> collector) throws Exception {
                        String[] splits = s.split(",");
                        collector.collect(new AdClick(
                                Long.valueOf(splits[0]),
                                Long.valueOf(splits[1]),
                                splits[2].trim(),
                                splits[3].trim(),
                                Long.valueOf(splits[4])
                                ));
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<AdClick>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<AdClick>() {
                    @Override
                    public long extractTimestamp(AdClick adClick, long l) {
                        return adClick.getTimestamp() * 1000;
                    }
                }));
        SingleOutputStreamOperator<AdClick> process = clickDataSource.keyBy(new KeySelector<AdClick, String>() {
            @Override
            public String getKey(AdClick adClick) throws Exception {
                return adClick.getAdId() + "-" + adClick.getUserId();
            }
        }).process(new FilterBlackListUser(100, blackWarningOutputTag));

        process.keyBy(new KeySelector<AdClick, String>() {
            @Override
            public String getKey(AdClick adClick) throws Exception {
                return adClick.getProvince();
            }
        }).timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AggregateFunction<AdClick, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(AdClick adClick, Long aLong) {
                        return aLong + 1;
                    }

                    @Override
                    public Long getResult(Long aLong) {
                        return aLong;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return aLong + acc1;
                    }
                }, new WindowFunction<Long, CountProvince, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<CountProvince> collector) throws Exception {
                        collector.collect(new CountProvince(String.valueOf(timeWindow.getEnd()), s, iterable.iterator().next()));
                    }
                });

        process.getSideOutput(blackWarningOutputTag).print();

        env.execute();
    }
}
