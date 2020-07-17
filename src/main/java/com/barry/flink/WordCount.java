package com.barry.flink;

import com.barry.flink.domain.WordWithCount;
import com.barry.flink.domain.WordWithCount;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

//nc64 -l -p 9000 -v
@Slf4j
public class WordCount {
    public static void main(String[] args) throws Exception {
        //定义socket的端口号
        int port;
        try{
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.err.println("没有指定port参数，使用默认值9000");
            port = 9000;
        }

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", port, "\n");

        //计算数据
        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word: splits) {
                    out.collect(new WordWithCount(word,1L));
                }
            }
        })//打平操作，把每行的单词转为<word,count>类型的数据
                .keyBy("word")//针对相同的word数据进行分组
                .timeWindow(Time.seconds(3),Time.seconds(1))//指定计算数据的窗口大小和滑动窗口大小
                .trigger(new Trigger<WordWithCount, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(WordWithCount element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        log.info("==1=={},{}", getDateTime(timestamp), element);
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        log.info("==2=="+getDateTime(time));
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        log.info("==3=="+getDateTime(time));
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                }).sum("count");

        //把数据打印到控制台
        windowCount.print()
                .setParallelism(1);//使用一个并行度
        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");
    }

    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * long转换成字符串日期
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String getDateTime(long longTime) {
        return dateToString(new Date(longTime), dateFormat);
    }

    /**
     * 日期类型转换成字符串类型
     * @param date 日期
     * @param dateFormat 日期格式
     * @return 日期字符串
     */
    public static String dateToString(Date date, DateFormat dateFormat) {
        return dateFormat.format(date);
    }
}