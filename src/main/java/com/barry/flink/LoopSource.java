package com.barry.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

@Slf4j
public class LoopSource implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        //try {
            //这句代码会加载spring
            //Class.forName(SpringContextUtils.class.getName());
        //} catch (ClassNotFoundException e) {
            //log.error("", e);
        //}
        while(true) {
            TimeUnit.MINUTES.sleep(1);
            ctx.collect("我是帅哥");
        }
    }

    @Override
    public void cancel() {
        /**
         * 注意这一步很重要,关闭springApplication, 不然cancel Job不会停止定时任务
         */
        //SpringContextUtils.shutdown();
    }
}
