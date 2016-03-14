package com.mls.flume

import java.net.InetAddress

import com.mls.flume.util.Constants
import org.apache.flume.instrumentation.util.JMXPollUtil
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

/**
  * Created by zhangzhikuan on 16/3/10.
  */
object TimeProcess {
  //打印日志
  private val logger = LoggerFactory.getLogger("RedisMonitorServer")
  //待收集的选项--sink:成功写出到存储的事件总数量
  private val attributeList = List("EventDrainSuccessCount")
  //保存历史记录的存储
  val valMap = collection.mutable.Map[String, Long]()
  //机器主机名
  private val hostName: String = InetAddress.getLocalHost.getHostName

  /**
    * 定时逻辑
    * @param redisHost redis的主机名
    * @param redisPort redis的端口
    * @param redisDB redis的数据库
    * @param time 时间窗口
    */
  def withRedis(redisHost: String, redisPort: Int, redisDB: Int)(time: Long): Unit = {
    //创建redis实例
    val redis = new Jedis(redisHost, redisPort)
    redis.select(redisDB) //选择redis库
    //时间戳
    val timeStr = Constants.date2String(time)
    try {
      //获取所有的信息
      val allMBeans: java.util.Map[String, java.util.Map[String, String]] = JMXPollUtil.getAllMBeans
      val it = allMBeans.keySet.iterator
      //开始遍历
      while (it.hasNext) {
        val component_key = it.next
        //组件也是一个map
        val attributeMap: java.util.Map[String, String] = allMBeans.get(component_key)
        //进行迭代
        val it$ = attributeMap.keySet().iterator()
        while (it$.hasNext) {
          val attribute_key = it$.next
          val attribute_value = attributeMap.get(attribute_key)
          send2Redis(redis, timeStr)(component_key, attribute_key, attribute_value)
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

  /**
    *
    * @param redis 存储的redis
    * @param timeStr 当前时间戳
    * @param component_key 组件key
    * @param attribute_key 属性key
    * @param attribute_value 属性值
    */
  private def send2Redis(redis: Jedis, timeStr: String)(component_key: String, attribute_key: String, attribute_value: String): Unit = {
    try {
      //判断是否需要收集
      if (attributeList.contains(attribute_key)) {
        //组件+属性=>value
        val flume_key = s"${component_key}.${attribute_key}"
        //value值
        val newValue = attribute_value.toLong
        //获取原有的值
        val oldValue = valMap.getOrElse(flume_key, 0L)
        //发送给redis
        val time_number = newValue - oldValue
        redis.hset(timeStr, s"${hostName}${Constants.TOPIC_SPLIT}${flume_key}", time_number.toString)
        //本地缓存的值
        valMap.put(flume_key, newValue)
        //打印日志
        logger.warn(s"${timeStr}.${flume_key}--oldValue[${oldValue}],newValue[${newValue}]增量[${time_number}]")
      }
    } catch {
      case e: Exception => logger.warn(s"Metric:[${component_key}.${attribute_key}:${attribute_value}]发送到redis失败", e);
    }

  }
}
