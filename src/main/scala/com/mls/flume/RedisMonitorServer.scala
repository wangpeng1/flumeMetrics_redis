package com.mls.flume

import org.apache.flume.Context
import org.apache.flume.instrumentation.MonitorService
import org.slf4j.LoggerFactory

/**
  * Created by zhangzhikuan on 16/3/9.
  */
class RedisMonitorServer extends MonitorService {
  //打印日志
  val logger = LoggerFactory.getLogger(classOf[RedisMonitorServer])
  //redis地址,域名
  var redisHost: String = _
  //redis端口
  var redisPort: Int = _
  //redis库
  var redisDB: Int = _
  //定时器
  val timer = new RecurringTimer(new SystemClock, 1 * 60 * 1000, TimeProcess.withRedis(redisHost, redisPort, redisDB)_, "RedisMonitorServer")

  //重写
  override def start(): Unit = {
    //清零,其实如果可以做到持久化会不会好点呢
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
    redisHost = context.getString("ip", "127.0.0.1")
    redisPort = context.getInteger("port", 6379)
    redisDB = context.getInteger("db", 0)
  }
}