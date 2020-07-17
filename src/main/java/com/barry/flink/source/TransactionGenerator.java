package com.barry.flink.source;

import com.barry.flink.dynamicrules.dynamicrules.Transaction;
import com.barry.flink.utils.DataGenerator;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
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
            Transaction event = new Transaction();
            event.setTransactionId(data.taxiId());
            event.setEventTime(data.startTime().toEpochMilli());
            event.setPayeeId(data.driverId());
            event.setBeneficiaryId(data.driverId()+1);
            if(id >= 150){
                event.setPaymentType(Transaction.PaymentType.CRD);
            } else {
                event.setPaymentType(Transaction.PaymentType.CSH);
            }
            event.setPaymentAmount(new BigDecimal(data.tip()));
            event.setIngestionTimestamp(rnd.nextLong());
            //Transaction event = Transaction.fromString(data.taxiId()+",2020-07-17 00:00:00,1001,1002,CSH,21.5,1,20");
            ctx.collect(event);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
