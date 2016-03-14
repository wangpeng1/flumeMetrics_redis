package com.mls.flume

import com.mls.flume.time.{RecurringTimer, SystemClock}
import org.apache.flume.Context
import org.apache.flume.instrumentation.MonitorService
import org.slf4j.LoggerFactory

/**
  * Created by zhangzhikuan on 16/3/9.
  */
class RedisMonitorServer extends MonitorService {
  //打印日志
  val logger = LoggerFactory.getLogger(classOf[RedisMonitorServer])
  //定时器
  var timer: RecurringTimer = _

  override def start(): Unit = {
    //清零,
    //持久化会不会更好点呢?
    //定时器重启

    TimeProcess.valMap.clear()
    //定时器关闭
    timer.start()
  }

  override def stop(): Unit = {
    //将保存的数据清理
    TimeProcess.valMap.clear()
    //定时器启动
    timer.stop(true)
  }

  override def configure(context: Context): Unit = {
    val redisHost = context.getString("ip", "127.0.0.1")
    val redisPort = context.getInteger("port", 6379)
    val redisDB = context.getInteger("db", 0)
    //定时器
    timer = new RecurringTimer(new SystemClock, 1 * 60 * 1000, TimeProcess.withRedis(redisHost, redisPort, redisDB), "RedisMonitorServer")
  }
}