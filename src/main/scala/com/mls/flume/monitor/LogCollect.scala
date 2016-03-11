package com.mls.flume.monitor

import java.text.SimpleDateFormat
import java.util.Date

import com.mls.flume.time.{RecurringTimer, SystemClock}
import com.mls.flume.util.Constants
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis


/**
  * Created by zhangzhikuan on 16/3/11.
  */
object LogCollect {
  //打印日志
  val logger = LoggerFactory.getLogger("FlumeMonitor")
  //时间模版
  val df = new SimpleDateFormat("yyyyMMddHH:mm")

  //执行函数
  def withRedis(redisHost: String, redisPort: Int, db: Int)(time: Long): Unit = {

    Thread.sleep(90 * 1000)

    val redis = new Jedis()
    val allMap: java.util.Map[String, String] = redis.hgetAll(df.format(new Date(time)))
    val resultMap = collection.mutable.Map[String, Long]()
    val it = allMap.keySet().iterator()
    while (it.hasNext) {
      val oldKey = it.next()
      val newKey = oldKey.split(Constants.TOPIC_SPLIT)(1)
      resultMap.put(newKey, allMap.get(oldKey).toLong + resultMap.getOrElse(newKey, 0L))
    }
    logger.warn(s"输出结果${resultMap}")

  }

  def main(args: Array[String]) {
    //redis的配置项
    val Array(redisHost, redisPort, redisDB) = args
    val timer = new RecurringTimer(new SystemClock, 5 * 60 * 1000, withRedis(redisHost, redisPort.toInt, redisDB.toInt), "logCollect")
    timer.start()
    Waiter.waitForStopOrError()
  }
}
