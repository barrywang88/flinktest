package com.barry.flink.actions

import java.util

import com.barry.flink.patterns.PatternLogin
import com.barry.flink.pojo.LoginEvent
import com.google.gson.Gson
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  /data2/flink-1.10.0/bin/flink run  -yD yarn.containers.vcores=1 -ytm 1024 -ynm LoginFailWithCep -m yarn-cluster   -p 2 --yarnslots 2  \
  -c com.barry.flink.actions.LoginFailWithCep  /data2/observatory-1.0-SNAPSHOT-jar-with-dependencies.jar --patternname 用户浏览
  topic
  groupid
  patternname
  patternstr
 */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromArgs(args)
    val sinkTopic = parameters.get("topic", "patternresult")
//    val groupid = parameters.get("groupid", "groupid")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventStream = env.readTextFile(getClass.getResource("/LoginLog.csv").getPath).map(x => {
        val dataArray = x.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong ,"{name:'zhangsan',age:12}")
      } )
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      } ).keyBy(_.userId)

    val patternStr = "begin('begin').where(eventType == fail && eventTime>1558430826 && info.age:int>=12).next('next').where(eventType == fail).within(Time.seconds(3))"
    val loginFailPattern = new PatternLogin(patternStr).pattern()

//    val loginFailPattern = Pattern.begin[LoginEvent]("begin").
//      where(_.eventType == "fail").next("next").where(_.eventType == "fail").within(Time.seconds(3))

    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)
    val loginFailDataStream:DataStream[PatternResult] = patternStream.select( new LoginFailMatch() )

    val strStream = loginFailDataStream.map(x=>{
      val gson = new Gson
      gson.toJson(x)
    })

    strStream.print()

   /* val kafkaProducer: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](
      sinkTopic,
      new SimpleStringSchema(),
      properties
    )
    strStream.print()
    strStream.filter(_ != null).addSink(kafkaProducer)
*/
    env.execute("login fail with cep job")
  }
}


case class PatternResult(uid:String,pt:Long,event_path:util.Map[String, util.List[LoginEvent]])

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, PatternResult]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): PatternResult = {
    val k = map.keySet().iterator().next()
    val v = map.get(k).iterator().next()
    PatternResult(v.userId.toString, java.lang.System.currentTimeMillis/1000, map)
  }
}