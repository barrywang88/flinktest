package com.barry.flink;

import com.barry.flink.dynamicrules.dynamicrules.Alert;
import com.barry.flink.dynamicrules.dynamicrules.Rule;
import com.barry.flink.dynamicrules.dynamicrules.RulesEvaluator;
import com.barry.flink.dynamicrules.dynamicrules.Transaction;
import com.barry.flink.dynamicrules.dynamicrules.functions.DynamicAlertFunction;
import com.barry.flink.dynamicrules.dynamicrules.functions.DynamicKeyFunction;
import com.barry.flink.source.RuleGenerator;
import com.barry.flink.source.TransactionGenerator;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName DynamicRules
 * @Description 动态规则处理
 * @Author wangxuexing
 * @Date 2020/7/16 16:49
 * @Version 1.0
 */
public class DynamicRules {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.failureRateRestart(1,// max failures per interval
                Time.of(5, TimeUnit.SECONDS), //time interval for measuring failure rate
                Time.of(1, TimeUnit.SECONDS))); //delay

        DataStreamSource<Transaction> transactionSource = env.addSource(new TransactionGenerator());
        DataStreamSource<Rule> ruleSource = env.addSource(new RuleGenerator());
        //将规则流广播到下游算子
        BroadcastStream<Rule> etlRuleBroadcastStream = ruleSource.broadcast(RulesEvaluator.Descriptors.rulesDescriptor);
        DataStream<Alert> alerts = transactionSource.connect(etlRuleBroadcastStream)
                .process(new DynamicKeyFunction())
                .keyBy(x -> x.getKey())
                .connect(etlRuleBroadcastStream)
                .process(new DynamicAlertFunction());

        alerts.print();
        env.execute("DynamicRules");
    }
}
