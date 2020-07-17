package com.barry.flink.source;

import com.barry.flink.dynamicrules.dynamicrules.Transaction;
import com.barry.flink.utils.DataGenerator;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class TransactionGenerator implements SourceFunction<Transaction> {
    private volatile boolean running = true;
    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        long id = 1;
        while (running) {
            DataGenerator data = new DataGenerator(id);
            id +=1;
            Transaction event = new Transaction();
            event.setTransactionId(id);
            event.setEventTime(data.startTime().toEpochMilli());
            event.setPayeeId(data.driverId());
            event.setBeneficiaryId(data.driverId()+1);
            if(id >= 5){
                event.setPaymentType(Transaction.PaymentType.CRD);
            } else {
                event.setPaymentType(Transaction.PaymentType.CSH);
            }
            event.setPaymentAmount(new BigDecimal(data.tip()));
            event.setIngestionTimestamp(id);
            event.setTotalFare(new BigDecimal(data.totalFare()));
            //Transaction event = Transaction.fromString(data.taxiId()+",2020-07-17 00:00:00,1001,1002,CSH,21.5,1,20");
            ctx.collect(event);
            //线程睡眠
            Thread.sleep(3*1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
