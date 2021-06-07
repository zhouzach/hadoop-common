package org.rabbit.spark.monitor

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerTaskEnd}


/**
 * can not listen org.rabbit.spark sql job, only org.rabbit.spark job
 * @param conf
 */
class SparkJobListener (conf: SparkConf) extends SparkListener with Logging {


  override def onApplicationStart(ev: SparkListenerApplicationStart): Unit = {
    println("AAA: Application_Start")
  }

  override def onApplicationEnd(ev: SparkListenerApplicationEnd): Unit = {

    println("AAA: Application_End")
    println(s"ev: ${ev.time} - ${ev.toString}")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val info = taskEnd.taskInfo
    // If stage attempt id is -1, it means the DAGScheduler had no idea which attempt this task
    // completion event is for. Let's just drop it here. This means we might have some speculation
    // tasks on the web ui that's never marked as complete.
    println(s"SparkListenerTaskEnd---------------------------")
    println(s"taskEnd: $taskEnd---------------------------")
    if (info != null && taskEnd.stageAttemptId != -1) {
      val errorMessage: Option[String] =
        taskEnd.reason match {
          case kill: TaskKilled =>
            Some(kill.toErrorString)
          case e: ExceptionFailure =>
            Some(e.toErrorString)
          case e: TaskFailedReason =>
            Some(e.toErrorString)
          case _ => None
        }

      println(s"errorMessage_spark: $errorMessage---------------------------")
      if (errorMessage.nonEmpty) {
        if (conf.getBoolean("enableSendEmailOnTaskFail", false)) {
          try {

//            implicit def stringToSeq(single: String): Seq[String] = Seq(single)
//            implicit def liftToOption[T](t: T): Option[T] = Some(t)

            MailHelper.send( new MailInfo (
              to = "769087026@qq.com":: Nil,
              subject = "org.rabbit.spark job monitor",
              message = errorMessage.get
            ))
            println(s"email_sent---------------------------")
            logInfo("email_sent_info")
          } catch {
            case e: Exception =>
              e.printStackTrace()
              logError(e.getLocalizedMessage)
              println(e.getLocalizedMessage)
          }
        }
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println(s"jobEnd: $jobEnd---------------------------")
  }
}
