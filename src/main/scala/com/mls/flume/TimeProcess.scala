package com.mls.flume

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flume.instrumentation.util.JMXPollUtil
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

/**
  * Created by zhangzhikuan on 16/3/10.
  */
object TimeProcess {
  //打印日志
  val logger = LoggerFactory.getLogger(classOf[RedisMonitorServer])
  //待收集的选项--sink:成功写出到存储的事件总数量
  val attributeList = List("EventDrainSuccessCount")
  //时间格式
  val df = new SimpleDateFormat("yyyyMMddHHmm")
  //保存历史记录的存储
  val valMap = collection.mutable.Map[String, Long]()
  //机器主机名
  val hostName: String = InetAddress.getLocalHost.getHostName

  //定时逻辑
  def withRedis(redisHost: String, redisPort: Int, redisDB: Int)(time: Long): Unit = {
    //创建redis实例
    val redis = new Jedis(redisHost, redisPort)
    redis.select(redisDB) //选择redis库

    //时间戳
    val timeStr = df.format(new Date(time))
    try {
      //获取所有的信息
      val t: java.util.Map[String, java.util.Map[String, String]] = JMXPollUtil.getAllMBeans
      val it = t.keySet.iterator
      //开始遍历
      while (it.hasNext) {
        val component_key = it.next
        //组件也是一个map
        val attributeMap: java.util.Map[String, String] = t.get(component_key)
        //进行迭代
        val it$ = attributeMap.keySet().iterator()
        while (it$.hasNext) {
          val attribute_key = it$.next
          //组件+属性=>value
          val flume_key = s"${component_key}.${attribute_key}"
          //发送redis的key
          val redis_time_key = s"${timeStr}::${flume_key}"
          //开始往redis写数据
          try {
            if (attributeList.contains(attribute_key)) {
              //value值
              val newValue = attributeMap.get(attribute_key).toLong
              //获取原有的值
              val oldValue = valMap.getOrElse(flume_key, 0L)
              //发送给redis
              val time_num = newValue - oldValue
              redis.hset(redis_time_key, hostName, time_num.toString)
              //本地缓存的值
              valMap.put(flume_key, newValue)
              //打印日志
              logger.warn(s"${redis_time_key}--oldValue[${oldValue}],newValue[${newValue}]增量[${time_num}]")
            }
          } catch {
            case e: Exception => logger.warn(s"Metric:Component[${component_key}]-Attribute[${attribute_key}]收集失败", e);
          }
        }
      }
    } catch {
      case t: Throwable => logger.error("未知错误", t);
    } finally {
      //判断redis是否为空,如果不为空,则关闭
      if (redis != null) {
        redis.close()
      }
    }
  }
}
