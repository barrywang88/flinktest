package com.barry.flink.patterns.conditions;


import com.barry.flink.patterns.Obj2Map;
import com.barry.flink.patterns.aviator.*;
import com.barry.flink.pojo.LoginEvent;
import com.googlecode.aviator.AviatorEvaluator;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.io.Serializable;
import java.util.Map;

public class LogEventCondition  extends SimpleCondition<LoginEvent> implements Serializable {
    private String script;

    static {
        AviatorEvaluator.addFunction(new GetFieldFunction());
        AviatorEvaluator.addFunction(new GetString());
        AviatorEvaluator.addFunction(new GetInt());
        AviatorEvaluator.addFunction(new GetLong());
        AviatorEvaluator.addFunction(new GetDouble());
    }

    //getField(uid)=\"uid\"
    public LogEventCondition(String script){
        this.script = script;
    }

    @Override
    public boolean filter(LoginEvent value) throws Exception {
        Map<String, Object> stringObjectMap = Obj2Map.objectToMap(value);
        boolean result = (Boolean) AviatorEvaluator.execute(script, stringObjectMap);
        return result;
    }

}