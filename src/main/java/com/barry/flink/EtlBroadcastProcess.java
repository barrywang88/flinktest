package com.barry.flink;

import com.barry.flink.source.GetJobConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName EtlBroadcastProcess
 * @Description 规则变动广播给所有流处理记录
 * @Author wangxuexing
 * @Date 2020/7/15 14:33
 * @Version 1.0
 */
@Slf4j
public class EtlBroadcastProcess {
    private final static int parallelism = 2;
    private final static long ruleId = 1;

    public static void main(String[] args) throws Exception {
        //定义socket的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("没有指定port参数，使用默认值9000");
            port = 9000;
        }

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.failureRateRestart(1,// max failures per interval
                Time.of(5, TimeUnit.SECONDS), //time interval for measuring failure rate
                Time.of(1, TimeUnit.SECONDS))); //delay

        //连接socket获取输入的数据
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", port, "\n");

        DataStreamSource<Map<String, GetJobConfig.JobConfig>> jobConfigStream = env.addSource(new GetJobConfig(ruleId));
        //定义MapState
        MapStateDescriptor<String, GetJobConfig.JobConfig> etlRules = new MapStateDescriptor<String, GetJobConfig.JobConfig>(
                "etl_rules",
                //Key类型
                BasicTypeInfo.STRING_TYPE_INFO,
                //Value类型
                TypeInformation.of(new TypeHint<GetJobConfig.JobConfig>() {}));
        //将规则流广播到下游算子
        BroadcastStream<Map<String, GetJobConfig.JobConfig>> etlRuleBroadcastStream = jobConfigStream.broadcast(etlRules);

        //规则流和数据流的结合
        DataStream<Tuple2<String, String>> outputStream = source.connect(etlRuleBroadcastStream)
                .process(new EtlBroadcastProcessFunction())
                .setParallelism(parallelism)
                .name("etl").uid("etl");

        outputStream.print();

        env.execute("EtlBroadcastProcess");

    }

    public static class EtlBroadcastProcessFunction
            extends BroadcastProcessFunction<String, Map<String, GetJobConfig.JobConfig>, Tuple2<String, String>> {
        //解析规则
        private GetJobConfig.JobConfig jobConfig;

        /**
         * 处理数据流
         * @param record
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(String record, ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            //record为要处理的数据，可利用获取到的规则jobConfig来检测收到的数据
            log.info("======record:{}", record);
            if(this.jobConfig != null){
                log.info("======jobRule:{}", this.jobConfig.getRule());
                //模拟修改规则后重启job
                if(this.jobConfig.getRule().equals("rule1")){
                    throw new Exception("重启触发器");
                }
            }

            out.collect(Tuple2.of(record, this.jobConfig.toString()));
        }

        /**
         * 获取规则流并缓存
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processBroadcastElement(Map<String, GetJobConfig.JobConfig> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            //value中为获取到的规则数据
            //缓存规则数据
            this.jobConfig = value.get("jobConfig");
        }
    }
}
