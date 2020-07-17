package com.barry.flink.source;

import com.barry.flink.dynamicrules.dynamicrules.Rule;
import com.barry.flink.dynamicrules.dynamicrules.RuleParser;
import com.barry.flink.utils.DataGenerator;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

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
        while (isRunning) {
            Random rnd = new Random(200);
            long id = 100+ rnd.nextInt(200);
            DataGenerator data = new DataGenerator(id);
            RuleParser ruleParser = new RuleParser();
            Rule rule =
                    ruleParser.fromString(data.taxiId()+",(active),(paymentType&payeeId),,(totalFare),(SUM),(>),(10),(20)");
            ctx.collect(rule);
            //线程睡眠
            Thread.sleep(2*60*1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
