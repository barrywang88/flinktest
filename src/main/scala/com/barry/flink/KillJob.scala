package com.barry.flink

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.Patterns
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.flink.runtime.messages.JobManagerMessages
import org.apache.flink.runtime.messages.JobManagerMessages.{CancelJob, RunningJobsStatus}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

object KillJob {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("flink", ConfigFactory.load.getConfig("akka")) //I debugged to get this path
    val jobManager = system.actorSelection("/user/taskmanager_0") //also got this akka path by debugging and getting the jobmanager akka url
    val responseRunningJobs = Patterns.ask(jobManager, JobManagerMessages.getRequestRunningJobsStatus, new FiniteDuration(10000, TimeUnit.MILLISECONDS))
    try {
      val result = Await.result(responseRunningJobs, new FiniteDuration(5000, TimeUnit.MILLISECONDS))
      if(result.isInstanceOf[RunningJobsStatus]){
        val runningJobs = result.asInstanceOf[RunningJobsStatus].getStatusMessages()
        val itr = runningJobs.iterator()
        while(itr.hasNext){
          val jobId = itr.next().getJobId
          val killResponse = Patterns.ask(jobManager, new CancelJob(jobId), new Timeout(new FiniteDuration(2000, TimeUnit.MILLISECONDS)));
          try {
            Await.result(killResponse, new FiniteDuration(2000, TimeUnit.MILLISECONDS))
          }
          catch {
            case e : Exception =>"Canceling the job with ID " + jobId + " failed." + e
          }
        }
      }
    }
    catch{
      case e : Exception => "Could not retrieve running jobs from the JobManager." + e
    }
  }
}