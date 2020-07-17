package com.barry.flink.source;

import com.barry.flink.domain.TaxiRide;
import com.barry.flink.dynamicrules.dynamicrules.Transaction;
import com.barry.flink.utils.DataGenerator;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @ClassName TransactionGenerator
 * @Description 交易生成器
 * @Author wangxuexing
 * @Date 2020/7/17 9:43
 * @Version 1.0
 */
public class TransactionGenerator implements SourceFunction<Transaction> {
    private volatile boolean running = true;
    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        while (running) {
            Random rnd = new Random(200);
            long id = 100+ rnd.nextInt(200);
            DataGenerator data = new DataGenerator(id);
            Transaction event1 = Transaction.fromString(data.taxiId()+",2020-07-17 00:00:00,1001,1002,CSH,21.5,1");
            ctx.collect(event1);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
