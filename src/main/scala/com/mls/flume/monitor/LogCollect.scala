package com.mls.flume.monitor

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


  //执行函数
  def withRedis(redisHost: String, redisPort: Int, db: Int)(time: Long): Unit = {

    //等待这么时间主要是为了保证所有的agent数据都已经入到redis中, 这里等待1分半
    Thread.sleep(90 * 1000)

    //redis客户端
    val redis = new Jedis()
    val resultMap = collection.mutable.Map[String, Long]()
    try {
      val allMap: java.util.Map[String, String] = redis.hgetAll(Constants.date2String(time))
      val it = allMap.keySet().iterator()
      while (it.hasNext) {
        val oldKey = it.next()
        val newKey = oldKey.split(Constants.TOPIC_SPLIT)(1)
        resultMap.put(newKey, allMap.get(oldKey).toLong + resultMap.getOrElse(newKey, 0L))
      }
    } catch {
      case e: Exception => logger.error("未知错误", e)
    } finally {
      redis.close()
    }
    logger.warn( s"""输出结果$resultMap""")

  }

  def main(args: Array[String]) {
    //redis的配置项
    val Array(redisHost, redisPort, redisDB) = args
    val timer = new RecurringTimer(new SystemClock, 5 * 60 * 1000, withRedis(redisHost, redisPort.toInt, redisDB.toInt), "logCollect")
    timer.start()
    Waiter.waitForStopOrError()
  }
}
