package com.barry.flink;

import com.barry.flink.domain.UserBehavior;
import com.barry.flink.domain.WordWithCount;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.HashMap;

/**
 * @ClassName PageView
 * @Description PV
 * @Author wangxuexing
 * @Date 2020/7/11 21:45
 * @Version 1.0
 */
@Slf4j
public class PageView {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String sourcePath = PageView.class.getResource("/UserBehavior.csv").getPath();
        DataStream<String> dataStream = env.readTextFile(sourcePath);
        dataStream.map(data -> {
            String[] cols = data.split(",");
            UserBehavior userBehavior = new UserBehavior();
            userBehavior.setUserId(Long.valueOf(cols[0]));
            userBehavior.setItemId(Long.valueOf(cols[1]));
            userBehavior.setCategoryId(Integer.valueOf(cols[2]));
            userBehavior.setBehavior(cols[3]);
            userBehavior.setTimestamp(Long.valueOf(cols[4]));
            return userBehavior;
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long previousElementTimestamp) {
                return element.getTimestamp()*1000L;
            }

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return null;
            }
        }) // 升序数据，直接分配时间戳
          .filter((FilterFunction<UserBehavior>) value -> value.getBehavior().equals("pv"))
          .map((MapFunction<UserBehavior, WordWithCount>) value -> new WordWithCount(value.getBehavior(), 1L))
          .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
          .sum("count");
        dataStream.print("pv count");
        env.execute("pv");
    }


}
