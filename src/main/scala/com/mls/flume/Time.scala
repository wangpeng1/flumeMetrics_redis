package com.mls.flume

/**
  * Created by zhangzhikuan on 16/3/9.
  */
object Time {
  def main(args: Array[String]) {
     val timer = new RecurringTimer(new SystemClock, 2*1000,time=>{println(time)},"time_ts")
    timer.start()
    Thread.sleep(10000);
  }
}
