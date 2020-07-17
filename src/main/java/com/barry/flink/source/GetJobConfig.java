package com.barry.flink.source;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class GetJobConfig extends RichSourceFunction<Map<String, GetJobConfig.JobConfig>> {
    private static String RULE1 = "rule1";
    private static String RULE2 = "rule2";
    private static Map<Long, String> RULES_MAP = Maps.newHashMap();
    static {
        RULES_MAP.put(1L, RULE1);
        RULES_MAP.put(2L, RULE2);
    }

    private volatile boolean isRunning = true;
    /**
     * 规则配置id
     */
    private Long jobHistoryId;

    public GetJobConfig(Long jobHistoryId) {
        this.jobHistoryId = jobHistoryId;
    }

    /**
     * 解析规则查询周期为1分钟
     */
    private Long duration = 3* 1000L;

    @Override
    public void run(SourceFunction.SourceContext<Map<String, JobConfig>> ctx) throws Exception {
        while (isRunning) {
            //从Redis或Mysql数据库获取配置
            String jobRule = RULES_MAP.get(jobHistoryId);
            //log.info("=====current jobHistoryId:{}, jobRule:{}", jobHistoryId, jobRule);
            //解析规则与业务逻辑相关请忽略
            if (!StringUtils.isEmpty(jobRule)) {
                JobConfig jobConfig = new JobConfig();
                jobConfig.setRule(jobRule);
                Map<String,JobConfig> jobConfigMap = new HashMap<>(12);
                jobConfigMap.put("jobConfig", jobConfig);
                //输出规则流
                ctx.collect(jobConfigMap);
            }
            //模拟修改规则
            if(jobHistoryId.longValue() == 1){
                jobHistoryId = 2L;
            } else if(jobHistoryId.longValue() == 2){
                jobHistoryId = 1L;
            }
            //线程睡眠
            Thread.sleep(duration);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Data
    public static class JobConfig{
        String rule;
    }
}
