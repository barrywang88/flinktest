package com.barry.flink.source;

import com.barry.flink.dynamicrules.dynamicrules.Rule;
import com.barry.flink.dynamicrules.dynamicrules.RuleParser;
import com.barry.flink.utils.DataGenerator;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Random;

/**
 * @ClassName TransactionGenerator
 * @Description 生成规则
 * @Author wangxuexing
 * @Date 2020/7/16 18:37
 * @Version 1.0
 */
public class RuleGenerator extends RichSourceFunction<Rule> {
    private volatile boolean isRunning = true;
    @Override
    public void run(SourceContext<Rule> ctx) throws Exception {
        int id = 1;
        while (isRunning) {
            DataGenerator data = new DataGenerator(id);
            id +=1;
            Rule rule = new Rule();
            rule.setRuleId(id);
            Arrays.stream(Rule.RuleState.values()).forEach(x-> {
                if(x.ordinal() == rule.getRuleId()%4){
                    rule.setRuleState(x);
                }
            });
            rule.setGroupingKeyNames(Lists.newArrayList("paymentType","payeeId"));
            rule.setAggregateFieldName("totalFare");
            Arrays.stream(Rule.AggregatorFunctionType.values()).forEach(x-> {
                if(x.ordinal() == data.driverId()%4){
                    rule.setAggregatorFunctionType(x);
                }
            });
            Arrays.stream(Rule.LimitOperatorType.values()).forEach(x-> {
                if(x.ordinal() == data.taxiId()%6){
                    rule.setLimitOperatorType(x);
                }
            });
            Arrays.stream(Rule.ControlType.values()).forEach(x-> {
                if(x.ordinal() == data.taxiId()%4){
                    rule.setControlType(x);
                }
            });
            rule.setLimit(new BigDecimal(data.totalFare()));
            rule.setWindowMinutes(id);
            //RuleParser ruleParser = new RuleParser();
//            Rule rule = ruleParser.fromString(data.taxiId()+",(active),(paymentType&payeeId),,(totalFare),(SUM),(>),(10),(20)");
            ctx.collect(rule);
            //线程睡眠
            Thread.sleep(5*1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
